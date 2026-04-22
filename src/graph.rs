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

#[derive(Clone, Debug)]
pub struct GraphEdgeSnapshot {
    pub from_node: String,
    pub from_port: &'static str,
    pub to_node: String,
    pub to_port: &'static str,
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

pub trait GraphNodeSpec {
    type Node: NodeDefinition;

    fn kind(&self) -> &'static str;
    fn export_node(&self, id: String) -> crate::pipeline::NodeSpec;
    fn into_parts(self) -> (Self::Node, <Self::Node as NodeDefinition>::Config);
}

pub struct NodeHandle<Node: NodeDefinition> {
    pub id: String,
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
    pub async fn send<T: Clone + Send + 'static>(
        &mut self,
        port: &'static str,
        value: T,
    ) -> Result<(), GraphError> {
        let senders = self
            .ports
            .remove(port)
            .ok_or(GraphError::MissingOutputPort {
                node: self.node_name,
                port,
            })?;

        let mut senders = senders
            .downcast::<Vec<mpsc::Sender<T>>>()
            .map(|boxed| *boxed)
            .map_err(|_| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("output port `{port}` had an unexpected runtime type"),
            })?;

        let sender_count = senders.len();
        for (index, sender) in senders.iter_mut().enumerate() {
            let payload = if index + 1 == sender_count {
                value.clone()
            } else {
                value.clone()
            };

            sender
                .send(payload)
                .await
                .map_err(|_| GraphError::NodeExecution {
                    node: self.node_name,
                    message: format!("output port `{port}` receiver was closed"),
                })?;
        }

        Ok(())
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
    next_id_by_kind: BTreeMap<&'static str, usize>,
    node_specs: Vec<crate::pipeline::NodeSpec>,
    edges: Vec<GraphEdgeSnapshot>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            channel_capacity: 8,
            next_id_by_kind: BTreeMap::new(),
            node_specs: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity;
        self
    }

    pub fn add<Spec: GraphNodeSpec>(
        &mut self,
        spec: Spec,
    ) -> NodeHandle<Spec::Node> {
        let next = self
            .next_id_by_kind
            .entry(spec.kind())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let assigned_id = format!("{}_{}", spec.kind(), *next);
        self.node_specs.push(spec.export_node(assigned_id.clone()));
        let (node, config) = spec.into_parts();
        self.add_node(assigned_id, node, config)
    }

    fn add_node<Node: NodeDefinition>(
        &mut self,
        assigned_id: String,
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
            id: assigned_id,
            input: Node::Input::ports(&factory),
            output: Node::Output::ports(&factory),
        }
    }

    pub fn connect<T: Send + 'static>(
        &mut self,
        source: OutputPort<T>,
        target: InputPort<T>,
    ) -> Result<(), GraphError> {
        let source_node_id = source.node_id.0;
        let source_port_name = source.name;
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
        let from_node = self
            .node_specs
            .get(source_node_id)
            .map(|node| node.id().to_owned())
            .ok_or_else(|| GraphError::Validation("source node metadata did not exist".into()))?;
        let to_node = self
            .node_specs
            .get(target.node_id.0)
            .map(|node| node.id().to_owned())
            .ok_or_else(|| GraphError::Validation("target node metadata did not exist".into()))?;
        self.edges.push(GraphEdgeSnapshot {
            from_node,
            from_port: source_port_name,
            to_node,
            to_port: target.name,
        });
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

    pub fn export_nodes(&self) -> &[crate::pipeline::NodeSpec] {
        &self.node_specs
    }

    pub fn edges(&self) -> &[GraphEdgeSnapshot] {
        &self.edges
    }

    fn attach_output<T: Send + 'static>(
        &mut self,
        source: OutputPort<T>,
    ) -> Result<(mpsc::Sender<T>, mpsc::Receiver<T>), GraphError> {
        let source_node = self
            .nodes
            .get_mut(source.node_id.0)
            .ok_or_else(|| GraphError::Validation("source node did not exist".into()))?;

        if let Some(existing) = source_node.outputs.get_mut(source.name) {
            let senders = existing
                .downcast_mut::<Vec<mpsc::Sender<T>>>()
                .ok_or_else(|| GraphError::Validation(format!(
                    "output port `{}` on node `{}` had an unexpected runtime type",
                    source.name, source_node.name
                )))?;

            let (sender, receiver) = mpsc::channel(self.channel_capacity);
            senders.push(sender);
            return Ok((senders.last().expect("sender inserted").clone(), receiver));
        }

        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        source_node.outputs.insert(source.name, Box::new(vec![sender.clone()]));
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
