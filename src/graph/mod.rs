use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub type NodeFuture = Pin<Box<dyn Future<Output = Result<(), GraphError>> + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeId(usize);

impl NodeId {
    fn new(raw: usize) -> Self {
        Self(raw)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputPort<T> {
    node_id: NodeId,
    name: &'static str,
    _marker: PhantomData<fn() -> T>,
}

impl<T> InputPort<T> {
    pub fn new(node_id: NodeId, name: &'static str) -> Self {
        Self {
            node_id,
            name,
            _marker: PhantomData,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputPort<T> {
    node_id: NodeId,
    name: &'static str,
    _marker: PhantomData<fn() -> T>,
}

impl<T> OutputPort<T> {
    pub fn new(node_id: NodeId, name: &'static str) -> Self {
        Self {
            node_id,
            name,
            _marker: PhantomData,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

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

pub struct NodeHandle<Ports> {
    pub id: NodeId,
    pub ports: Ports,
}

pub struct NodeContext {
    node_name: &'static str,
    inputs: BTreeMap<&'static str, Box<dyn Any + Send>>,
    outputs: BTreeMap<&'static str, Box<dyn Any + Send>>,
}

impl NodeContext {
    pub fn take_input<T: Send + 'static>(
        &mut self,
        port: &'static str,
    ) -> Result<mpsc::Receiver<T>, GraphError> {
        let receiver = self.inputs.remove(port).ok_or(GraphError::MissingInputPort {
            node: self.node_name,
            port,
        })?;

        receiver.downcast::<mpsc::Receiver<T>>().map(|boxed| *boxed).map_err(|_| {
            GraphError::NodeExecution {
                node: self.node_name,
                message: format!("input port `{port}` had an unexpected runtime type"),
            }
        })
    }

    pub fn take_output<T: Send + 'static>(
        &mut self,
        port: &'static str,
    ) -> Result<mpsc::Sender<T>, GraphError> {
        let sender = self.outputs.remove(port).ok_or(GraphError::MissingOutputPort {
            node: self.node_name,
            port,
        })?;

        sender.downcast::<mpsc::Sender<T>>().map(|boxed| *boxed).map_err(|_| {
            GraphError::NodeExecution {
                node: self.node_name,
                message: format!("output port `{port}` had an unexpected runtime type"),
            }
        })
    }
}

struct NodeRegistration {
    name: &'static str,
    task: Box<dyn FnOnce(NodeContext) -> NodeFuture + Send>,
    inputs: BTreeMap<&'static str, Box<dyn Any + Send>>,
    outputs: BTreeMap<&'static str, Box<dyn Any + Send>>,
}

impl NodeRegistration {
    fn new(
        name: &'static str,
        task: impl FnOnce(NodeContext) -> NodeFuture + Send + 'static,
    ) -> Self {
        Self {
            name,
            task: Box::new(task),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        }
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

    pub fn add_node<Ports>(
        &mut self,
        name: &'static str,
        ports: impl FnOnce(NodeId) -> Ports,
        task: impl FnOnce(NodeContext) -> NodeFuture + Send + 'static,
    ) -> NodeHandle<Ports> {
        let id = NodeId::new(self.nodes.len());
        let handle = NodeHandle {
            id,
            ports: ports(id),
        };

        self.nodes.push(NodeRegistration::new(name, task));
        handle
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

        let (_bound_sender, receiver) = sender;
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
        source_node.outputs.insert(source.name, Box::new(sender.clone()));
        Ok((sender, receiver))
    }

    pub async fn run(self) -> Result<(), GraphError> {
        let mut tasks = JoinSet::new();

        for registration in self.nodes {
            let context = NodeContext {
                node_name: registration.name,
                inputs: registration.inputs,
                outputs: registration.outputs,
            };
            let task = (registration.task)(context);
            tasks.spawn(task);
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

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}
