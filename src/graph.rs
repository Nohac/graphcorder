use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use facet::Facet;
use facet_json_schema::JsonSchema;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

type NodeFuture = Pin<Box<dyn Future<Output = Result<(), GraphError>> + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct NodeId(usize);

#[derive(Debug)]
pub enum GraphError {
    MissingInputPort {
        node: &'static str,
        port: &'static str,
    },
    MissingOutputPort {
        node: &'static str,
        port: &'static str,
    },
    PortAlreadyConnected {
        node: &'static str,
        port: &'static str,
    },
    NodeExecution {
        node: &'static str,
        message: String,
    },
    TaskJoin(String),
    Validation(String),
}

impl Display for GraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingInputPort { node, port } => {
                write!(f, "missing input port `{port}` on node `{node}`")
            }
            Self::MissingOutputPort { node, port } => {
                write!(f, "missing output port `{port}` on node `{node}`")
            }
            Self::PortAlreadyConnected { node, port } => {
                write!(f, "port `{port}` on node `{node}` is already connected")
            }
            Self::NodeExecution { node, message } => {
                write!(f, "node `{node}` failed: {message}")
            }
            Self::TaskJoin(message) => write!(f, "task join failure: {message}"),
            Self::Validation(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for GraphError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputPort<T> {
    node_id: NodeId,
    name: &'static str,
    _marker: PhantomData<fn() -> T>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputPort<T> {
    node_id: NodeId,
    name: &'static str,
    _marker: PhantomData<fn() -> T>,
}

#[derive(Clone, Copy, Debug)]
pub struct PortFactory {
    node_id: NodeId,
}

impl PortFactory {
    pub fn input<T>(&self, name: &'static str) -> InputPort<T> {
        InputPort {
            node_id: self.node_id,
            name,
            _marker: PhantomData,
        }
    }

    pub fn output<T>(&self, name: &'static str) -> OutputPort<T> {
        OutputPort {
            node_id: self.node_id,
            name,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PortSchema {
    pub name: &'static str,
    pub schema: JsonSchema,
}

pub trait NodeInputs: Send + Sized + 'static {
    type Ports;

    fn ports(factory: &PortFactory) -> Self::Ports;
    fn schema() -> Vec<PortSchema>;
    fn no_runtime_inputs() -> bool {
        false
    }
    fn receive(runtime: &mut InputRuntime)
    -> impl Future<Output = Result<Self, GraphError>> + Send;
}

pub trait NodeOutputs: Send + Sized + 'static {
    type Ports;

    fn ports(factory: &PortFactory) -> Self::Ports;
    fn schema() -> Vec<PortSchema>;
    fn send(
        self,
        runtime: &mut OutputRuntime,
    ) -> impl Future<Output = Result<(), GraphError>> + Send;
}

pub trait NodeDefinition: Send + Sync + 'static {
    type Config: Clone + Send + Sync + Facet<'static> + 'static;
    type Input: NodeInputs;
    type Output: NodeOutputs;

    const KIND: &'static str;

    fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
    ) -> impl Future<Output = Result<Self::Output, GraphError>> + Send;
}

pub struct NodeHandle<Node: NodeDefinition> {
    pub input: <Node::Input as NodeInputs>::Ports,
    pub output: <Node::Output as NodeOutputs>::Ports,
}

pub struct InputRuntime {
    node_name: &'static str,
    ports: BTreeMap<&'static str, Box<dyn Any + Send>>,
}

impl InputRuntime {
    pub async fn receive<T: Send + 'static>(
        &mut self,
        port: &'static str,
    ) -> Result<T, GraphError> {
        let receiver = self
            .ports
            .remove(port)
            .ok_or(GraphError::MissingInputPort {
                node: self.node_name,
                port,
            })?;

        let mut receiver = receiver
            .downcast::<mpsc::Receiver<T>>()
            .map(|boxed| *boxed)
            .map_err(|_| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("input port `{port}` had an unexpected runtime type"),
            })?;

        receiver
            .recv()
            .await
            .ok_or_else(|| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("input port `{port}` closed before producing a value"),
            })
    }
}

pub struct OutputRuntime {
    node_name: &'static str,
    ports: BTreeMap<&'static str, Box<dyn Any + Send>>,
}

impl OutputRuntime {
    pub async fn send<T: Send + 'static>(
        &mut self,
        port: &'static str,
        value: T,
    ) -> Result<(), GraphError> {
        let sender = self
            .ports
            .remove(port)
            .ok_or(GraphError::MissingOutputPort {
                node: self.node_name,
                port,
            })?;

        let sender = sender
            .downcast::<mpsc::Sender<T>>()
            .map(|boxed| *boxed)
            .map_err(|_| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("output port `{port}` had an unexpected runtime type"),
            })?;

        sender
            .send(value)
            .await
            .map_err(|_| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("output port `{port}` receiver was closed"),
            })
    }
}

struct NodeRegistration {
    name: &'static str,
    task: Box<dyn FnOnce(InputRuntime, OutputRuntime) -> NodeFuture + Send>,
    inputs: BTreeMap<&'static str, Box<dyn Any + Send>>,
    outputs: BTreeMap<&'static str, Box<dyn Any + Send>>,
}

pub struct Graph {
    nodes: Vec<NodeRegistration>,
}

impl Graph {
    pub async fn run(self) -> Result<(), GraphError> {
        let mut tasks = JoinSet::new();

        for registration in self.nodes {
            let inputs = InputRuntime {
                node_name: registration.name,
                ports: registration.inputs,
            };
            let outputs = OutputRuntime {
                node_name: registration.name,
                ports: registration.outputs,
            };
            tasks.spawn((registration.task)(inputs, outputs));
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(GraphError::TaskJoin(error.to_string())),
            }
        }

        Ok(())
    }
}

pub struct GraphBuilder {
    nodes: Vec<NodeRegistration>,
    channel_capacity: usize,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            channel_capacity: 8,
        }
    }

    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity;
        self
    }

    pub fn add_node<Node: NodeDefinition>(
        &mut self,
        node: Node,
        config: Node::Config,
    ) -> NodeHandle<Node> {
        let node_id = NodeId(self.nodes.len());
        let factory = PortFactory { node_id };

        self.nodes.push(NodeRegistration {
            name: Node::KIND,
            task: Box::new(move |mut inputs, mut outputs| {
                Box::pin(async move {
                    let input = Node::Input::receive(&mut inputs).await?;
                    let output = node.run(input, &config).await?;
                    output.send(&mut outputs).await
                })
            }),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        });

        NodeHandle {
            input: Node::Input::ports(&factory),
            output: Node::Output::ports(&factory),
        }
    }

    pub fn connect<T: Send + 'static>(
        &mut self,
        source: OutputPort<T>,
        target: InputPort<T>,
    ) -> Result<(), GraphError> {
        let sender = self.attach_output(source)?;
        let target_node = self
            .nodes
            .get_mut(target.node_id.0)
            .ok_or_else(|| GraphError::Validation("target node did not exist".into()))?;

        if target_node.inputs.contains_key(target.name) {
            return Err(GraphError::PortAlreadyConnected {
                node: target_node.name,
                port: target.name,
            });
        }

        let (_sender, receiver) = sender;
        target_node.inputs.insert(target.name, Box::new(receiver));
        Ok(())
    }

    pub fn capture_output<T: Send + 'static>(
        &mut self,
        source: OutputPort<T>,
    ) -> Result<mpsc::Receiver<T>, GraphError> {
        let (_sender, receiver) = self.attach_output(source)?;
        Ok(receiver)
    }

    pub fn build(self) -> Graph {
        Graph { nodes: self.nodes }
    }

    fn attach_output<T: Send + 'static>(
        &mut self,
        source: OutputPort<T>,
    ) -> Result<(mpsc::Sender<T>, mpsc::Receiver<T>), GraphError> {
        let source_node = self
            .nodes
            .get_mut(source.node_id.0)
            .ok_or_else(|| GraphError::Validation("source node did not exist".into()))?;

        if source_node.outputs.contains_key(source.name) {
            return Err(GraphError::PortAlreadyConnected {
                node: source_node.name,
                port: source.name,
            });
        }

        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        source_node
            .outputs
            .insert(source.name, Box::new(sender.clone()));
        Ok((sender, receiver))
    }
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeInputs for () {
    type Ports = ();

    fn ports(_factory: &PortFactory) -> Self::Ports {}

    fn schema() -> Vec<PortSchema> {
        Vec::new()
    }

    fn no_runtime_inputs() -> bool {
        true
    }

    async fn receive(_runtime: &mut InputRuntime) -> Result<Self, GraphError> {
        Ok(())
    }
}
