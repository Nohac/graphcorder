use std::any::Any;
use std::any::TypeId;
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

#[derive(Debug, Eq, PartialEq)]
pub struct InputPort<T> {
    node_id: NodeId,
    name: &'static str,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Copy for InputPort<T> {}

impl<T> Clone for InputPort<T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct OutputPort<T> {
    node_id: NodeId,
    name: &'static str,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Copy for OutputPort<T> {}

impl<T> Clone for OutputPort<T> {
    fn clone(&self) -> Self {
        *self
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortCardinality {
    Single,
    Many,
    Fixed(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaticPortInfo {
    pub name: &'static str,
    pub cardinality: PortCardinality,
    pub required: bool,
}

#[derive(Clone, Debug)]
pub struct PortSchema {
    pub name: &'static str,
    pub schema: JsonSchema,
    pub cardinality: PortCardinality,
}

#[derive(Clone, Debug, Facet)]
pub struct GraphSpec<R> {
    pub nodes: Vec<R>,
    pub edges: Vec<EdgeSpec>,
}

#[derive(Clone, Debug, Facet)]
pub struct GraphNode<T> {
    pub id: String,
    pub config: T,
}

impl<T> GraphNode<T> {
    pub fn new(id: String, config: T) -> Self {
        Self { id, config }
    }
}

#[derive(Clone, Debug, Facet)]
pub struct EdgeSpec {
    pub from: PortRef,
    pub to: PortRef,
}

#[derive(Clone, Debug, Facet)]
pub struct PortRef {
    pub node: String,
    pub port: String,
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

pub trait StaticInputPorts {
    const PORTS: &'static [StaticPortInfo];
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

pub trait StaticOutputPorts {
    const PORTS: &'static [StaticPortInfo];
}

pub trait PortValue: Clone + Send + Sync + Facet<'static> + 'static {}

macro_rules! impl_port_value {
    ($($ty:ty),* $(,)?) => {
        $(impl PortValue for $ty {})*
    };
}

impl_port_value!(f32, f64, usize, u32, u64, i32, i64, bool, String);

impl<T: PortValue> PortValue for Vec<T> {}

impl<T: PortValue, const N: usize> PortValue for [T; N] {}

pub trait InputPortValue: Send + Sized + 'static {
    type EdgeValue: Send + 'static;

    fn schema(name: &'static str) -> PortSchema;
    fn receive(
        runtime: &mut InputRuntime,
        port: &'static str,
    ) -> impl Future<Output = Result<Self, GraphError>> + Send;
}

impl<T: PortValue> InputPortValue for T {
    type EdgeValue = T;

    fn schema(name: &'static str) -> PortSchema {
        PortSchema {
            name,
            schema: facet_json_schema::schema_for::<T>(),
            cardinality: PortCardinality::Single,
        }
    }

    async fn receive(runtime: &mut InputRuntime, port: &'static str) -> Result<Self, GraphError> {
        runtime.receive_one(port).await
    }
}

pub trait OutputPortValue: Send + Sized + 'static {
    type EdgeValue: Clone + Send + 'static;

    fn schema(name: &'static str) -> PortSchema;
    fn send(
        self,
        runtime: &mut OutputRuntime,
        port: &'static str,
    ) -> impl Future<Output = Result<(), GraphError>> + Send;
}

impl<T: PortValue> OutputPortValue for T {
    type EdgeValue = T;

    fn schema(name: &'static str) -> PortSchema {
        PortSchema {
            name,
            schema: facet_json_schema::schema_for::<T>(),
            cardinality: PortCardinality::Single,
        }
    }

    async fn send(self, runtime: &mut OutputRuntime, port: &'static str) -> Result<(), GraphError> {
        runtime.send(port, self).await
    }
}

pub trait NodeDefinition: Send + Sync + 'static {
    type Config: Clone + Send + Sync + Facet<'static> + 'static;
    type Input: NodeInputs;
    type Output: NodeOutputs;

    fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
    ) -> impl Future<Output = Result<Self::Output, GraphError>> + Send;
}

pub trait NodeMeta {
    const KIND: &'static str;
}

pub trait GraphNodeSpec {
    type Node: NodeDefinition + NodeMeta;
    type Registry;

    fn export_node(&self, id: String) -> Self::Registry;
    fn into_parts(self) -> (Self::Node, <Self::Node as NodeDefinition>::Config);
}

pub trait StaticNodeDsl {
    type Config;
    type Node: NodeDefinition + NodeMeta;
    type Spec: GraphNodeSpec<Node = Self::Node>;

    fn from_config(config: Self::Config) -> Self::Spec;
}

pub const fn has_port(ports: &[StaticPortInfo], name: &str) -> bool {
    let mut index = 0;
    while index < ports.len() {
        if const_str_eq(ports[index].name, name) {
            return true;
        }
        index += 1;
    }
    false
}

pub const fn is_single_port(ports: &[StaticPortInfo], name: &str) -> bool {
    let mut index = 0;
    while index < ports.len() {
        if const_str_eq(ports[index].name, name) {
            return matches!(ports[index].cardinality, PortCardinality::Single);
        }
        index += 1;
    }
    false
}

pub const fn has_missing_required_ports(ports: &[StaticPortInfo], connected: &[&str]) -> bool {
    let mut port_index = 0;
    while port_index < ports.len() {
        let port = ports[port_index];
        if port.required {
            let mut found = false;
            let mut connected_index = 0;
            while connected_index < connected.len() {
                if const_str_eq(connected[connected_index], port.name) {
                    found = true;
                    break;
                }
                connected_index += 1;
            }
            if !found {
                return true;
            }
        }
        port_index += 1;
    }
    false
}

pub const fn has_duplicate_single_connections(
    ports: &[StaticPortInfo],
    connected: &[&str],
) -> bool {
    let mut connected_index = 0;
    while connected_index < connected.len() {
        let current = connected[connected_index];
        if is_single_port(ports, current) {
            let mut seen = 0;
            let mut inner_index = 0;
            while inner_index < connected.len() {
                if const_str_eq(connected[inner_index], current) {
                    seen += 1;
                    if seen > 1 {
                        return true;
                    }
                }
                inner_index += 1;
            }
        }
        connected_index += 1;
    }

    false
}

pub const fn only_port_name(ports: &[StaticPortInfo]) -> Option<&'static str> {
    if ports.len() == 1 {
        Some(ports[0].name)
    } else {
        None
    }
}

pub const fn const_str_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();

    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }

    true
}

pub struct ErasedInputPort<R> {
    type_id: TypeId,
    inner: Box<dyn Any + Send>,
    connect: fn(&mut GraphBuilder<R>, &dyn Any, &dyn Any) -> Result<(), GraphError>,
}

impl<R: RegisteredNodeSpec> ErasedInputPort<R> {
    pub fn new<T: Send + 'static>(port: InputPort<T>) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            inner: Box::new(port),
            connect: |builder, source, target| {
                let source = source
                    .downcast_ref::<OutputPort<T>>()
                    .ok_or_else(|| GraphError::Validation("output port type mismatch".into()))?;
                let target = target
                    .downcast_ref::<InputPort<T>>()
                    .ok_or_else(|| GraphError::Validation("input port type mismatch".into()))?;
                builder.connect(*source, *target)
            },
        }
    }
}

pub struct ErasedOutputPort {
    type_id: TypeId,
    inner: Box<dyn Any + Send>,
}

impl ErasedOutputPort {
    pub fn new<T: Send + 'static>(port: OutputPort<T>) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            inner: Box::new(port),
        }
    }
}

pub trait ErasedInputPorts {
    fn input_port<R: RegisteredNodeSpec>(&self, name: &str) -> Option<ErasedInputPort<R>>;
}

pub trait ErasedOutputPorts {
    fn output_port(&self, name: &str) -> Option<ErasedOutputPort>;
}

trait ErasedBuiltNode<R: RegisteredNodeSpec>: Send {
    fn input_port(&self, name: &str) -> Option<ErasedInputPort<R>>;
    fn output_port(&self, name: &str) -> Option<ErasedOutputPort>;
}

struct BuiltNodeAdapter<I, O> {
    input: I,
    output: O,
}

impl<R, I, O> ErasedBuiltNode<R> for BuiltNodeAdapter<I, O>
where
    R: RegisteredNodeSpec,
    I: ErasedInputPorts + Send,
    O: ErasedOutputPorts + Send,
{
    fn input_port(&self, name: &str) -> Option<ErasedInputPort<R>> {
        self.input.input_port::<R>(name)
    }

    fn output_port(&self, name: &str) -> Option<ErasedOutputPort> {
        self.output.output_port(name)
    }
}

pub struct BuiltGraphNode<R: RegisteredNodeSpec> {
    inner: Box<dyn ErasedBuiltNode<R>>,
}

impl<R: RegisteredNodeSpec> BuiltGraphNode<R> {
    pub fn new<I, O>(input: I, output: O) -> Self
    where
        I: ErasedInputPorts + Send + 'static,
        O: ErasedOutputPorts + Send + 'static,
    {
        Self {
            inner: Box::new(BuiltNodeAdapter { input, output }),
        }
    }

    pub fn from_handle<Node>(handle: NodeHandle<Node>) -> Self
    where
        Node: NodeDefinition,
        <Node::Input as NodeInputs>::Ports: ErasedInputPorts + Send + 'static,
        <Node::Output as NodeOutputs>::Ports: ErasedOutputPorts + Send + 'static,
    {
        Self::new(handle.input, handle.output)
    }

    fn connect_to(
        &self,
        builder: &mut GraphBuilder<R>,
        from_port: &str,
        target: &BuiltGraphNode<R>,
        to_port: &str,
    ) -> Result<(), GraphError> {
        let source = self
            .inner
            .output_port(from_port)
            .ok_or_else(|| GraphError::Validation(format!("missing output port `{from_port}`")))?;
        let target = target
            .inner
            .input_port(to_port)
            .ok_or_else(|| GraphError::Validation(format!("missing input port `{to_port}`")))?;

        if source.type_id != target.type_id {
            return Err(GraphError::Validation(format!(
                "type mismatch connecting `{from_port}` to `{to_port}`"
            )));
        }

        (target.connect)(builder, source.inner.as_ref(), target.inner.as_ref())
    }
}

pub trait NodeRegistryEntry: Clone + Send + Sync + Facet<'static> + 'static {
    fn id(&self) -> &str;
    fn add_to_builder<R>(&self, builder: &mut GraphBuilder<R>) -> BuiltGraphNode<R>
    where
        Self: Into<R>,
        R: RegisteredNodeSpec;
}

pub trait RegisteredNodeSpec: Clone + Send + Sync + Facet<'static> + 'static {
    fn id(&self) -> &str;
    fn add_to_builder(&self, builder: &mut GraphBuilder<Self>) -> BuiltGraphNode<Self>;
}

pub struct NodeHandle<Node: NodeDefinition> {
    pub id: String,
    pub input: <Node::Input as NodeInputs>::Ports,
    pub output: <Node::Output as NodeOutputs>::Ports,
}

pub struct InputRuntime {
    node_name: &'static str,
    ports: BTreeMap<&'static str, Vec<Box<dyn Any + Send>>>,
}

impl InputRuntime {
    async fn take_receivers<T: Send + 'static>(
        &mut self,
        port: &'static str,
    ) -> Result<Vec<mpsc::Receiver<T>>, GraphError> {
        let receivers = self
            .ports
            .remove(port)
            .ok_or(GraphError::MissingInputPort {
                node: self.node_name,
                port,
            })?;

        receivers
            .into_iter()
            .map(|receiver| {
                receiver
                    .downcast::<mpsc::Receiver<T>>()
                    .map(|boxed| *boxed)
                    .map_err(|_| GraphError::NodeExecution {
                        node: self.node_name,
                        message: format!("input port `{port}` had an unexpected runtime type"),
                    })
            })
            .collect()
    }

    pub async fn receive_one<T: Send + 'static>(
        &mut self,
        port: &'static str,
    ) -> Result<T, GraphError> {
        let mut receivers = self.take_receivers(port).await?;
        if receivers.len() != 1 {
            return Err(GraphError::NodeExecution {
                node: self.node_name,
                message: format!("input port `{port}` expected exactly one connection"),
            });
        }

        let mut receiver = receivers.remove(0);

        receiver
            .recv()
            .await
            .ok_or_else(|| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("input port `{port}` closed before producing a value"),
            })
    }

    pub async fn receive_many<T: Send + 'static>(
        &mut self,
        port: &'static str,
    ) -> Result<Vec<T>, GraphError> {
        let mut values = Vec::new();
        for mut receiver in self.take_receivers(port).await? {
            let value = receiver
                .recv()
                .await
                .ok_or_else(|| GraphError::NodeExecution {
                    node: self.node_name,
                    message: format!("input port `{port}` closed before producing a value"),
                })?;
            values.push(value);
        }
        Ok(values)
    }

    pub async fn receive_fixed<T: Send + 'static>(
        &mut self,
        port: &'static str,
        expected: usize,
    ) -> Result<Vec<T>, GraphError> {
        let receivers = self.take_receivers::<T>(port).await?;
        if receivers.len() != expected {
            return Err(GraphError::NodeExecution {
                node: self.node_name,
                message: format!(
                    "input port `{port}` expected {expected} connections but got {}",
                    receivers.len()
                ),
            });
        }

        let mut values = Vec::with_capacity(expected);
        for mut receiver in receivers {
            let value = receiver
                .recv()
                .await
                .ok_or_else(|| GraphError::NodeExecution {
                    node: self.node_name,
                    message: format!("input port `{port}` closed before producing a value"),
                })?;
            values.push(value);
        }
        Ok(values)
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
    input_schemas: BTreeMap<&'static str, PortSchema>,
    inputs: BTreeMap<&'static str, Vec<Box<dyn Any + Send>>>,
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

pub struct GraphBuilder<R> {
    nodes: Vec<NodeRegistration>,
    channel_capacity: usize,
    next_id_by_kind: BTreeMap<&'static str, usize>,
    node_specs: Vec<R>,
    edges: Vec<GraphEdgeSnapshot>,
}

impl<R: RegisteredNodeSpec> GraphBuilder<R> {
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

    pub fn add<Spec: GraphNodeSpec>(&mut self, spec: Spec) -> NodeHandle<Spec::Node>
    where
        Spec::Registry: Into<R>,
    {
        let kind = <Spec::Node as NodeMeta>::KIND;
        let next = self
            .next_id_by_kind
            .entry(kind)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let assigned_id = format!("{}_{}", kind, *next);
        self.node_specs
            .push(spec.export_node(assigned_id.clone()).into());
        let (node, config) = spec.into_parts();
        self.add_node(assigned_id, node, config)
    }

    fn add_node<Node: NodeDefinition + NodeMeta>(
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
            input_schemas: Node::Input::schema()
                .into_iter()
                .map(|schema| (schema.name, schema))
                .collect(),
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

        let cardinality = target_node
            .input_schemas
            .get(target.name)
            .ok_or(GraphError::MissingInputPort {
                node: target_node.name,
                port: target.name,
            })?
            .cardinality
            .clone();

        let existing_connection_count = target_node
            .inputs
            .get(target.name)
            .map(|connections| connections.len())
            .unwrap_or(0);

        match cardinality {
            PortCardinality::Single if existing_connection_count > 0 => {
                return Err(GraphError::PortAlreadyConnected {
                    node: target_node.name,
                    port: target.name,
                });
            }
            PortCardinality::Fixed(limit) if existing_connection_count >= limit => {
                return Err(GraphError::Validation(format!(
                    "input port `{}` on node `{}` accepts at most {limit} connections",
                    target.name, target_node.name
                )));
            }
            _ => {}
        }

        let (_sender, receiver) = sender;
        target_node
            .inputs
            .entry(target.name)
            .or_default()
            .push(Box::new(receiver));
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

    pub fn connect_named(
        &mut self,
        source: &BuiltGraphNode<R>,
        source_port: &str,
        target: &BuiltGraphNode<R>,
        target_port: &str,
    ) -> Result<(), GraphError> {
        source.connect_to(self, source_port, target, target_port)
    }

    pub fn build(self) -> Graph {
        Graph { nodes: self.nodes }
    }

    pub fn export_nodes(&self) -> &[R] {
        &self.node_specs
    }

    pub fn edges(&self) -> &[GraphEdgeSnapshot] {
        &self.edges
    }

    pub fn graph_spec(&self) -> GraphSpec<R> {
        let nodes = self.export_nodes().to_vec();
        let edges = self
            .edges()
            .iter()
            .cloned()
            .map(|edge| EdgeSpec {
                from: PortRef {
                    node: edge.from_node,
                    port: edge.from_port.into(),
                },
                to: PortRef {
                    node: edge.to_node,
                    port: edge.to_port.into(),
                },
            })
            .collect();

        GraphSpec { nodes, edges }
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
                .ok_or_else(|| {
                    GraphError::Validation(format!(
                        "output port `{}` on node `{}` had an unexpected runtime type",
                        source.name, source_node.name
                    ))
                })?;

            let (sender, receiver) = mpsc::channel(self.channel_capacity);
            senders.push(sender);
            return Ok((senders.last().expect("sender inserted").clone(), receiver));
        }

        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        source_node
            .outputs
            .insert(source.name, Box::new(vec![sender.clone()]));
        Ok((sender, receiver))
    }
}

impl<R: RegisteredNodeSpec> Default for GraphBuilder<R> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Graphcorder<R> {
    _marker: PhantomData<fn() -> R>,
}

impl<R> Graphcorder<R> {
    pub fn builder(&self) -> GraphBuilder<R>
    where
        R: RegisteredNodeSpec,
    {
        GraphBuilder::new()
    }

    pub fn graph_schema(&self) -> JsonSchema
    where
        R: RegisteredNodeSpec,
    {
        facet_json_schema::schema_for::<GraphSpec<R>>()
    }

    pub fn build_graph_from_spec(&self, spec: GraphSpec<R>) -> Result<Graph, GraphError>
    where
        R: RegisteredNodeSpec,
    {
        let mut builder = GraphBuilder::new();
        let mut nodes = BTreeMap::new();

        for node in &spec.nodes {
            nodes.insert(node.id().to_owned(), node.add_to_builder(&mut builder));
        }

        for edge in &spec.edges {
            let source = nodes.get(&edge.from.node).ok_or_else(|| {
                GraphError::Validation(format!("missing source node `{}`", edge.from.node))
            })?;
            let target = nodes.get(&edge.to.node).ok_or_else(|| {
                GraphError::Validation(format!("missing target node `{}`", edge.to.node))
            })?;
            source.connect_to(&mut builder, &edge.from.port, target, &edge.to.port)?;
        }

        Ok(builder.build())
    }
}

pub fn init<R>() -> Graphcorder<R> {
    Graphcorder {
        _marker: PhantomData,
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

impl ErasedInputPorts for () {
    fn input_port<R: RegisteredNodeSpec>(&self, _name: &str) -> Option<ErasedInputPort<R>> {
        None
    }
}

impl StaticInputPorts for () {
    const PORTS: &'static [StaticPortInfo] = &[];
}

impl NodeOutputs for () {
    type Ports = ();

    fn ports(_factory: &PortFactory) -> Self::Ports {}

    fn schema() -> Vec<PortSchema> {
        Vec::new()
    }

    async fn send(self, _runtime: &mut OutputRuntime) -> Result<(), GraphError> {
        Ok(())
    }
}

impl ErasedOutputPorts for () {
    fn output_port(&self, _name: &str) -> Option<ErasedOutputPort> {
        None
    }
}

impl StaticOutputPorts for () {
    const PORTS: &'static [StaticPortInfo] = &[];
}
