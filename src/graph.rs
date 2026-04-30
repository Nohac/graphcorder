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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SingleInputPorts<T: InputPortValue> {
    pub value: InputPort<T>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SingleOutputPorts<T: OutputPortValue> {
    pub value: OutputPort<T>,
}

pub trait SingleInputPortHandle {
    type Port: InputPortValue;

    fn single_input_port(&self) -> InputPort<Self::Port>;
}

pub trait SingleOutputPortHandle {
    type Port: OutputPortValue;

    fn single_output_port(&self) -> OutputPort<Self::Port>;
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
    fn initialize(runtime: &mut OutputRuntime) -> Self;
    fn finalize(
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

impl<T: InputPortValue> NodeInputs for T {
    type Ports = SingleInputPorts<T>;

    fn ports(factory: &PortFactory) -> Self::Ports {
        SingleInputPorts {
            value: factory.input("value"),
        }
    }

    fn schema() -> Vec<PortSchema> {
        vec![T::schema("value")]
    }

    async fn receive(runtime: &mut InputRuntime) -> Result<Self, GraphError> {
        T::receive(runtime, "value").await
    }
}

impl<T: InputPortValue> ErasedInputPorts for SingleInputPorts<T> {
    fn input_port(&self, name: &str) -> Option<ErasedInputPort> {
        match name {
            "value" => Some(ErasedInputPort::new(self.value)),
            _ => None,
        }
    }
}

impl<T: InputPortValue> StaticInputPorts for T {
    const PORTS: &'static [StaticPortInfo] = &[StaticPortInfo {
        name: "value",
        cardinality: PortCardinality::Single,
        required: true,
    }];
}

impl<T: InputPortValue> SingleInputPortHandle for SingleInputPorts<T> {
    type Port = T;

    fn single_input_port(&self) -> InputPort<Self::Port> {
        self.value
    }
}

/// A streaming port value: producers push values with [`send`](Stream::send) during
/// `run()`; consumers receive them one at a time via [`next`](Stream::next).
///
/// The optional const parameter `N` sets the mpsc channel buffer capacity for this port.
/// `N = 0` (the default) uses the capacity configured on [`GraphBuilder`].
pub struct Stream<T: Clone + Send + 'static, const N: usize = 0> {
    inner: StreamInner<T>,
}

enum StreamInner<T: Clone + Send + 'static> {
    Output(Vec<mpsc::Sender<T>>),
    Input(mpsc::Receiver<T>),
}

impl<T: Clone + Send + 'static, const N: usize> Stream<T, N> {
    pub(crate) fn from_senders(senders: Vec<mpsc::Sender<T>>) -> Self {
        Self {
            inner: StreamInner::Output(senders),
        }
    }

    pub(crate) fn from_receiver(receiver: mpsc::Receiver<T>) -> Self {
        Self {
            inner: StreamInner::Input(receiver),
        }
    }

    /// Push a value to all downstream nodes. Call this from the producing node's `run()`.
    pub async fn send(&mut self, value: T) -> Result<(), GraphError> {
        let StreamInner::Output(senders) = &mut self.inner else {
            panic!("Stream::send() called on consumer-side stream");
        };
        for sender in senders.iter_mut() {
            sender
                .send(value.clone())
                .await
                .map_err(|_| GraphError::Validation("stream receiver was closed".into()))?;
        }
        Ok(())
    }

    /// Receive the next value from upstream. Returns `None` when the producer has finished.
    pub async fn next(&mut self) -> Option<T> {
        let StreamInner::Input(receiver) = &mut self.inner else {
            panic!("Stream::next() called on producer-side stream");
        };
        receiver.recv().await
    }
}

// Safety: Vec<mpsc::Sender<T>>: Send when T: Send; mpsc::Receiver<T>: Send when T: Send.
unsafe impl<T: Clone + Send + 'static, const N: usize> Send for Stream<T, N> {}

pub trait InputPortValue: Send + Sized + 'static {
    type EdgeValue: Send + 'static;
    /// The item type carried by the underlying mpsc channel. For scalar ports this is
    /// the value type itself; for `Stream<T, N>` it is `T`.
    type ChannelItem: Clone + Send + 'static;

    fn schema(name: &'static str) -> PortSchema;
    fn receive(
        runtime: &mut InputRuntime,
        port: &'static str,
    ) -> impl Future<Output = Result<Self, GraphError>> + Send;
}

impl<T: PortValue> InputPortValue for T {
    type EdgeValue = T;
    type ChannelItem = T;

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

impl<T: PortValue, const N: usize> InputPortValue for Stream<T, N> {
    type EdgeValue = Stream<T, N>;
    type ChannelItem = T;

    fn schema(name: &'static str) -> PortSchema {
        PortSchema {
            name,
            schema: facet_json_schema::schema_for::<T>(),
            cardinality: PortCardinality::Single,
        }
    }

    async fn receive(runtime: &mut InputRuntime, port: &'static str) -> Result<Self, GraphError> {
        let mut receivers = runtime.take_receivers::<T>(port).await?;
        if receivers.len() != 1 {
            return Err(GraphError::NodeExecution {
                node: runtime.node_name,
                message: format!("stream input port `{port}` expected exactly one connection"),
            });
        }
        Ok(Stream::from_receiver(receivers.remove(0)))
    }
}

pub trait OutputPortValue: Send + Sized + 'static {
    /// Used for TypeId-based port-type matching. Distinct per port value type.
    type EdgeValue: Send + 'static;
    /// The item type flowing through the underlying mpsc channel.
    type ChannelItem: Clone + Send + 'static;

    fn schema(name: &'static str) -> PortSchema;

    /// Per-port channel buffer capacity override. `None` uses the builder's default.
    /// Override this for `Stream<T, N>` ports to use a fixed capacity of `N`.
    fn channel_capacity() -> Option<usize> {
        None
    }

    /// Called before `run()` to extract pre-wired channels from the runtime and
    /// initialize this field in the output struct. Scalar ports return `Default::default()`.
    fn initialize_field(runtime: &mut OutputRuntime, port: &'static str) -> Self;

    /// Called after `run()` to flush any buffered scalar values. Stream ports are no-ops.
    fn finalize_field(
        self,
        runtime: &mut OutputRuntime,
        port: &'static str,
    ) -> impl Future<Output = Result<(), GraphError>> + Send;
}

impl<T: PortValue + Default> OutputPortValue for T {
    type EdgeValue = T;
    type ChannelItem = T;

    fn schema(name: &'static str) -> PortSchema {
        PortSchema {
            name,
            schema: facet_json_schema::schema_for::<T>(),
            cardinality: PortCardinality::Single,
        }
    }

    fn initialize_field(_runtime: &mut OutputRuntime, _port: &'static str) -> Self {
        T::default()
    }

    async fn finalize_field(
        self,
        runtime: &mut OutputRuntime,
        port: &'static str,
    ) -> Result<(), GraphError> {
        runtime.send(port, self).await
    }
}

impl<T: PortValue + Default, const N: usize> OutputPortValue for Stream<T, N> {
    type EdgeValue = Stream<T, N>;
    type ChannelItem = T;

    fn schema(name: &'static str) -> PortSchema {
        PortSchema {
            name,
            schema: facet_json_schema::schema_for::<T>(),
            cardinality: PortCardinality::Single,
        }
    }

    fn channel_capacity() -> Option<usize> {
        (N > 0).then_some(N)
    }

    fn initialize_field(runtime: &mut OutputRuntime, port: &'static str) -> Self {
        Stream::from_senders(runtime.take_senders::<T>(port))
    }

    async fn finalize_field(
        self,
        _runtime: &mut OutputRuntime,
        _port: &'static str,
    ) -> Result<(), GraphError> {
        // Senders drop here, signaling EOF to all connected consumers.
        Ok(())
    }
}

impl<T: OutputPortValue> NodeOutputs for T {
    type Ports = SingleOutputPorts<T>;

    fn ports(factory: &PortFactory) -> Self::Ports {
        SingleOutputPorts {
            value: factory.output("value"),
        }
    }

    fn schema() -> Vec<PortSchema> {
        vec![T::schema("value")]
    }

    fn initialize(runtime: &mut OutputRuntime) -> Self {
        T::initialize_field(runtime, "value")
    }

    async fn finalize(self, runtime: &mut OutputRuntime) -> Result<(), GraphError> {
        T::finalize_field(self, runtime, "value").await
    }
}

impl<T: OutputPortValue> ErasedOutputPorts for SingleOutputPorts<T> {
    fn output_port(&self, name: &str) -> Option<ErasedOutputPort> {
        match name {
            "value" => Some(ErasedOutputPort::new(self.value)),
            _ => None,
        }
    }
}

impl<T: OutputPortValue> StaticOutputPorts for T {
    const PORTS: &'static [StaticPortInfo] = &[StaticPortInfo {
        name: "value",
        cardinality: PortCardinality::Single,
        required: true,
    }];
}

impl<T: OutputPortValue> SingleOutputPortHandle for SingleOutputPorts<T> {
    type Port = T;

    fn single_output_port(&self) -> OutputPort<Self::Port> {
        self.value
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
        output: &mut Self::Output,
    ) -> impl Future<Output = Result<(), GraphError>> + Send;
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

#[repr(C)]
#[derive(Clone, Debug, Facet)]
pub enum ConstantValue {
    F32(f32),
    F64(f64),
    Usize(usize),
    U32(u32),
    U64(u64),
    I32(i32),
    I64(i64),
    Bool(bool),
    String(String),
    List(#[facet(recursive_type)] Vec<ConstantValue>),
}

#[repr(C)]
#[derive(Clone, Debug, Eq, PartialEq, Facet)]
pub enum ConstantKind {
    F32,
    F64,
    Usize,
    U32,
    U64,
    I32,
    I64,
    Bool,
    String,
    List(#[facet(recursive_type)] Box<ConstantKind>),
    MixedList,
    EmptyList,
}

macro_rules! impl_constant_value_from {
    ($($variant:ident => $ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for ConstantValue {
                fn from(value: $ty) -> Self {
                    Self::$variant(value)
                }
            }
        )*
    };
}

impl_constant_value_from!(
    F32 => f32,
    F64 => f64,
    Usize => usize,
    U32 => u32,
    U64 => u64,
    I32 => i32,
    I64 => i64,
    Bool => bool,
    String => String,
);

macro_rules! impl_constant_value_from_list {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<Vec<$ty>> for ConstantValue {
                fn from(value: Vec<$ty>) -> Self {
                    Self::List(value.into_iter().map(ConstantValue::from).collect())
                }
            }
        )*
    };
}

impl_constant_value_from_list!(f32, f64, usize, u32, u64, i32, i64, bool, String);

impl ConstantValue {
    pub fn kind(&self) -> ConstantKind {
        match self {
            Self::F32(_) => ConstantKind::F32,
            Self::F64(_) => ConstantKind::F64,
            Self::Usize(_) => ConstantKind::Usize,
            Self::U32(_) => ConstantKind::U32,
            Self::U64(_) => ConstantKind::U64,
            Self::I32(_) => ConstantKind::I32,
            Self::I64(_) => ConstantKind::I64,
            Self::Bool(_) => ConstantKind::Bool,
            Self::String(_) => ConstantKind::String,
            Self::List(values) => {
                let mut kinds = values.iter().map(Self::kind);
                match kinds.next() {
                    None => ConstantKind::EmptyList,
                    Some(first) => {
                        if kinds.all(|kind| kind == first) {
                            ConstantKind::List(Box::new(first))
                        } else {
                            ConstantKind::MixedList
                        }
                    }
                }
            }
        }
    }
}

impl PortValue for ConstantValue {}

impl Default for ConstantValue {
    fn default() -> Self {
        Self::F32(0.0)
    }
}

#[derive(Clone, Debug, Facet)]
pub struct ConstantGraphNode {
    pub id: String,
    pub value: ConstantValue,
}

#[derive(Clone, Debug, Facet)]
pub struct ConstantConfig<T> {
    pub value: T,
}

pub struct ConstantTyped<T>(PhantomData<fn() -> T>);

#[derive(Clone, Debug)]
pub struct ConstantTypedSpec<T> {
    value: T,
}

struct ToConstantValueNode<T>(PhantomData<fn() -> T>);

pub fn constant<T>(value: T) -> ConstantTypedSpec<T::Value>
where
    T: IntoConstantSpec,
{
    value.into_constant_spec()
}

pub trait ConstantElement:
    PortValue + Default + Into<ConstantValue> + Clone + Send + Sync + 'static
{
}

macro_rules! impl_constant_element {
    ($($ty:ty),* $(,)?) => {
        $(impl ConstantElement for $ty {})*
    };
}

impl_constant_element!(f32, f64, usize, u32, u64, i32, i64, bool, String);

pub trait PrimitiveConstant:
    PortValue + Default + Into<ConstantValue> + Clone + Send + Sync + 'static
{
}

impl<T: ConstantElement> PrimitiveConstant for T {}
impl PrimitiveConstant for ConstantValue {}
impl<T> PrimitiveConstant for Vec<T>
where
    T: ConstantElement,
    ConstantValue: From<Vec<T>>,
{
}

pub trait IntoConstantSpec {
    type Value: PrimitiveConstant;

    fn into_constant_spec(self) -> ConstantTypedSpec<Self::Value>;
}

impl<T: PrimitiveConstant> IntoConstantSpec for T {
    type Value = T;

    fn into_constant_spec(self) -> ConstantTypedSpec<Self::Value> {
        ConstantTypedSpec { value: self }
    }
}

impl<T, const N: usize> IntoConstantSpec for [T; N]
where
    T: ConstantElement,
    ConstantValue: From<Vec<T>>,
{
    type Value = Vec<T>;

    fn into_constant_spec(self) -> ConstantTypedSpec<Self::Value> {
        ConstantTypedSpec {
            value: Vec::from(self),
        }
    }
}

impl<T, const N: usize> IntoConstantSpec for &[T; N]
where
    T: ConstantElement,
    ConstantValue: From<Vec<T>>,
{
    type Value = Vec<T>;

    fn into_constant_spec(self) -> ConstantTypedSpec<Self::Value> {
        ConstantTypedSpec {
            value: self.to_vec(),
        }
    }
}

impl<T> IntoConstantSpec for &[T]
where
    T: ConstantElement,
    ConstantValue: From<Vec<T>>,
{
    type Value = Vec<T>;

    fn into_constant_spec(self) -> ConstantTypedSpec<Self::Value> {
        ConstantTypedSpec {
            value: self.to_vec(),
        }
    }
}

pub trait TryFromConstantValue: PortValue + Default {
    fn try_from_constant_value(value: ConstantValue) -> Option<Self>;
}

macro_rules! impl_try_from_constant_value {
    ($($variant:ident => $ty:ty),* $(,)?) => {
        $(
            impl TryFromConstantValue for $ty {
                fn try_from_constant_value(value: ConstantValue) -> Option<Self> {
                    match value {
                        ConstantValue::$variant(value) => Some(value),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_try_from_constant_value!(
    F32 => f32,
    F64 => f64,
    Usize => usize,
    U32 => u32,
    U64 => u64,
    I32 => i32,
    I64 => i64,
    Bool => bool,
    String => String,
);

impl TryFromConstantValue for ConstantValue {
    fn try_from_constant_value(value: ConstantValue) -> Option<Self> {
        Some(value)
    }
}

macro_rules! impl_try_from_constant_value_list {
    ($($variant:ident => $ty:ty),* $(,)?) => {
        $(
            impl TryFromConstantValue for Vec<$ty> {
                fn try_from_constant_value(value: ConstantValue) -> Option<Self> {
                    match value {
                        ConstantValue::List(values) => values
                            .into_iter()
                            .map(|value| match value {
                                ConstantValue::$variant(value) => Some(value),
                                _ => None,
                            })
                            .collect(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_try_from_constant_value_list!(
    F32 => f32,
    F64 => f64,
    Usize => usize,
    U32 => u32,
    U64 => u64,
    I32 => i32,
    I64 => i64,
    Bool => bool,
    String => String,
);

pub trait ConnectFromConstantSource<T: PrimitiveConstant>: InputPortValue {
    fn connect_from_constant_source<R: RegisteredNodeSpec>(
        builder: &mut GraphBuilder<R>,
        source: OutputPort<T>,
        target: InputPort<Self>,
    ) -> Result<(), GraphError>;
}

macro_rules! impl_direct_constant_source {
    ($($ty:ty),* $(,)?) => {
        $(
            impl ConnectFromConstantSource<$ty> for $ty {
                fn connect_from_constant_source<R: RegisteredNodeSpec>(
                    builder: &mut GraphBuilder<R>,
                    source: OutputPort<$ty>,
                    target: InputPort<Self>,
                ) -> Result<(), GraphError> {
                    builder.connect(source, target)
                }
            }
        )*
    };
}

impl_direct_constant_source!(f32, f64, usize, u32, u64, i32, i64, bool, String);

impl<T> ConnectFromConstantSource<Vec<T>> for Vec<T>
where
    T: ConstantElement,
    ConstantValue: From<Vec<T>>,
{
    fn connect_from_constant_source<R: RegisteredNodeSpec>(
        builder: &mut GraphBuilder<R>,
        source: OutputPort<Vec<T>>,
        target: InputPort<Self>,
    ) -> Result<(), GraphError> {
        builder.connect(source, target)
    }
}

impl<T> ConnectFromConstantSource<T> for ConstantValue
where
    T: PrimitiveConstant,
{
    fn connect_from_constant_source<R: RegisteredNodeSpec>(
        builder: &mut GraphBuilder<R>,
        source: OutputPort<T>,
        target: InputPort<Self>,
    ) -> Result<(), GraphError> {
        builder.connect_typed_into_constant_value(source, target)
    }
}

impl<T> NodeMeta for ConstantTyped<T> {
    const KIND: &'static str = "constant";
}

impl<T> NodeMeta for ToConstantValueNode<T> {
    const KIND: &'static str = "__constant_value_bridge";
}

impl<T> NodeDefinition for ConstantTyped<T>
where
    T: PrimitiveConstant,
{
    type Config = ConstantConfig<T>;
    type Input = ();
    type Output = T;

    async fn run(
        &self,
        _input: Self::Input,
        config: &Self::Config,
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        *output = config.value.clone();
        Ok(())
    }
}

impl<T> GraphNodeSpec for ConstantTypedSpec<T>
where
    T: PrimitiveConstant,
{
    type Node = ConstantTyped<T>;
    type Registry = ConstantGraphNode;

    fn export_node(&self, id: String) -> Self::Registry {
        ConstantGraphNode {
            id,
            value: self.value.clone().into(),
        }
    }

    fn into_parts(self) -> (Self::Node, <Self::Node as NodeDefinition>::Config) {
        (
            ConstantTyped(PhantomData),
            ConstantConfig { value: self.value },
        )
    }
}

impl<T> NodeDefinition for ToConstantValueNode<T>
where
    T: PrimitiveConstant,
{
    type Config = ();
    type Input = T;
    type Output = ConstantValue;

    async fn run(
        &self,
        input: Self::Input,
        _config: &Self::Config,
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        *output = input.into();
        Ok(())
    }
}

impl NodeRegistryEntry for ConstantGraphNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder<R>(&self, builder: &mut GraphBuilder<R>) -> BuiltGraphNode<R>
    where
        Self: Into<R>,
        R: RegisteredNodeSpec,
    {
        match &self.value {
            ConstantValue::F32(value) => BuiltGraphNode::from_handle(builder.add(constant(*value))),
            ConstantValue::F64(value) => BuiltGraphNode::from_handle(builder.add(constant(*value))),
            ConstantValue::Usize(value) => {
                BuiltGraphNode::from_handle(builder.add(constant(*value)))
            }
            ConstantValue::U32(value) => BuiltGraphNode::from_handle(builder.add(constant(*value))),
            ConstantValue::U64(value) => BuiltGraphNode::from_handle(builder.add(constant(*value))),
            ConstantValue::I32(value) => BuiltGraphNode::from_handle(builder.add(constant(*value))),
            ConstantValue::I64(value) => BuiltGraphNode::from_handle(builder.add(constant(*value))),
            ConstantValue::Bool(value) => {
                BuiltGraphNode::from_handle(builder.add(constant(*value)))
            }
            ConstantValue::String(value) => {
                BuiltGraphNode::from_handle(builder.add(constant(value.clone())))
            }
            ConstantValue::List(_) => match self.value.kind() {
                ConstantKind::List(kind) => match *kind {
                    ConstantKind::F32 => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<f32>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match f32 values"),
                        )),
                    ),
                    ConstantKind::F64 => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<f64>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match f64 values"),
                        )),
                    ),
                    ConstantKind::Usize => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<usize>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match usize values"),
                        )),
                    ),
                    ConstantKind::U32 => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<u32>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match u32 values"),
                        )),
                    ),
                    ConstantKind::U64 => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<u64>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match u64 values"),
                        )),
                    ),
                    ConstantKind::I32 => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<i32>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match i32 values"),
                        )),
                    ),
                    ConstantKind::I64 => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<i64>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match i64 values"),
                        )),
                    ),
                    ConstantKind::Bool => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<bool>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match bool values"),
                        )),
                    ),
                    ConstantKind::String => BuiltGraphNode::from_handle(
                        builder.add(constant(
                            Vec::<String>::try_from_constant_value(self.value.clone())
                                .expect("constant list kind should match string values"),
                        )),
                    ),
                    _ => BuiltGraphNode::from_handle(builder.add(constant(self.value.clone()))),
                },
                ConstantKind::MixedList | ConstantKind::EmptyList => {
                    BuiltGraphNode::from_handle(builder.add(constant(self.value.clone())))
                }
                _ => BuiltGraphNode::from_handle(builder.add(constant(self.value.clone()))),
            },
        }
    }
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

pub const fn validate_static_port_exists(ports: &[StaticPortInfo], name: &str) {
    if !has_port(ports, name) {
        panic!("unknown static graph port");
    }
}

pub const fn validate_static_implicit_port(ports: &[StaticPortInfo], is_source: bool) {
    if only_port_name(ports).is_none() {
        if is_source {
            panic!("implicit source port requires exactly one output port");
        } else {
            panic!("implicit target port requires exactly one input port");
        }
    }
}

pub const fn validate_static_input_connections(ports: &[StaticPortInfo], connected: &[&str]) {
    if has_duplicate_single_connections(ports, connected) {
        panic!("duplicate connection to single input port");
    }
    if has_missing_required_ports(ports, connected) {
        panic!("node is missing required input connections");
    }
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

/// Type-erased input port. Carries the channel-item TypeId for compatibility matching.
pub struct ErasedInputPort {
    channel_item_type_id: TypeId,
    target_node_idx: usize,
    target_port_name: &'static str,
}

impl ErasedInputPort {
    pub fn new<T: InputPortValue>(port: InputPort<T>) -> Self {
        Self {
            channel_item_type_id: TypeId::of::<T::ChannelItem>(),
            target_node_idx: port.node_id.0,
            target_port_name: port.name,
        }
    }
}

type AttachFn = fn(
    &mut [NodeRegistration],
    usize,
    &'static str,
    usize,
) -> Result<Box<dyn Any + Send>, GraphError>;

/// Type-erased output port. Carries the channel-item TypeId and a monomorphised
/// function pointer that wires the mpsc sender into the source node's output runtime.
pub struct ErasedOutputPort {
    channel_item_type_id: TypeId,
    source_node_idx: usize,
    source_port_name: &'static str,
    capacity_override: Option<usize>,
    attach: AttachFn,
}

impl ErasedOutputPort {
    pub fn new<T: OutputPortValue>(port: OutputPort<T>) -> Self {
        Self {
            channel_item_type_id: TypeId::of::<T::ChannelItem>(),
            source_node_idx: port.node_id.0,
            source_port_name: port.name,
            capacity_override: T::channel_capacity(),
            attach: attach_fn_for::<T::ChannelItem>,
        }
    }
}

fn attach_fn_for<T: Clone + Send + 'static>(
    nodes: &mut [NodeRegistration],
    node_idx: usize,
    port_name: &'static str,
    capacity: usize,
) -> Result<Box<dyn Any + Send>, GraphError> {
    let node = nodes
        .get_mut(node_idx)
        .ok_or_else(|| GraphError::Validation("source node did not exist".into()))?;

    if let Some(existing) = node.outputs.get_mut(port_name) {
        let senders = existing
            .downcast_mut::<Vec<mpsc::Sender<T>>>()
            .ok_or_else(|| {
                GraphError::Validation(format!(
                    "output port `{port_name}` on node `{}` had an unexpected runtime type",
                    node.name
                ))
            })?;
        let (sender, receiver) = mpsc::channel(capacity);
        senders.push(sender);
        return Ok(Box::new(receiver));
    }

    let (sender, receiver) = mpsc::channel::<T>(capacity);
    node.outputs.insert(port_name, Box::new(vec![sender]));
    Ok(Box::new(receiver))
}

pub trait ErasedInputPorts {
    fn input_port(&self, name: &str) -> Option<ErasedInputPort>;
}

pub trait ErasedOutputPorts {
    fn output_port(&self, name: &str) -> Option<ErasedOutputPort>;
}

trait ErasedBuiltNode<R: RegisteredNodeSpec>: Send {
    fn input_port(&self, name: &str) -> Option<ErasedInputPort>;
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
    fn input_port(&self, name: &str) -> Option<ErasedInputPort> {
        self.input.input_port(name)
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
        let target_port = target
            .inner
            .input_port(to_port)
            .ok_or_else(|| GraphError::Validation(format!("missing input port `{to_port}`")))?;

        builder.connect_erased(&source, &target_port)
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

impl<Node> NodeHandle<Node>
where
    Node: NodeDefinition,
    <Node::Input as NodeInputs>::Ports: SingleInputPortHandle,
{
    pub fn single_input_port(
        &self,
    ) -> InputPort<<<Node::Input as NodeInputs>::Ports as SingleInputPortHandle>::Port> {
        self.input.single_input_port()
    }
}

impl<Node> NodeHandle<Node>
where
    Node: NodeDefinition,
    <Node::Output as NodeOutputs>::Ports: SingleOutputPortHandle,
{
    pub fn single_output_port(
        &self,
    ) -> OutputPort<<<Node::Output as NodeOutputs>::Ports as SingleOutputPortHandle>::Port> {
        self.output.single_output_port()
    }
}

pub struct InputRuntime {
    pub(crate) node_name: &'static str,
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

        let senders = senders
            .downcast::<Vec<mpsc::Sender<T>>>()
            .map(|boxed| *boxed)
            .map_err(|_| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("output port `{port}` had an unexpected runtime type"),
            })?;

        let (last, rest) = senders.split_last().ok_or(GraphError::MissingOutputPort {
            node: self.node_name,
            port,
        })?;
        for sender in rest {
            sender
                .send(value.clone())
                .await
                .map_err(|_| GraphError::NodeExecution {
                    node: self.node_name,
                    message: format!("output port `{port}` receiver was closed"),
                })?;
        }
        last.send(value)
            .await
            .map_err(|_| GraphError::NodeExecution {
                node: self.node_name,
                message: format!("output port `{port}` receiver was closed"),
            })?;

        Ok(())
    }

    /// Extract the senders for a stream output port so they can be moved into `Stream::from_senders`.
    /// Returns an empty vec if the port has no downstream connections.
    pub fn take_senders<T: Clone + Send + 'static>(
        &mut self,
        port: &'static str,
    ) -> Vec<mpsc::Sender<T>> {
        self.ports
            .remove(port)
            .and_then(|b| b.downcast::<Vec<mpsc::Sender<T>>>().ok())
            .map(|b| *b)
            .unwrap_or_default()
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
                    let mut output = Node::Output::initialize(&mut outputs);
                    node.run(input, &config, &mut output).await?;
                    output.finalize(&mut outputs).await
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

    fn add_internal_node<Node: NodeDefinition + NodeMeta>(
        &mut self,
        node: Node,
        config: Node::Config,
    ) -> NodeHandle<Node> {
        let assigned_id = format!("__internal_{}_{}", Node::KIND, self.nodes.len());
        self.add_node(assigned_id, node, config)
    }

    /// Connect an output port to a compatible input port. The output and input types must
    /// share the same `ChannelItem` — this allows `f32` → `Stream<f32>` and
    /// `Stream<f32, M>` → `Stream<f32, N>` in addition to same-type connections.
    pub fn connect<Out: OutputPortValue, In: InputPortValue<ChannelItem = Out::ChannelItem>>(
        &mut self,
        source: OutputPort<Out>,
        target: InputPort<In>,
    ) -> Result<(), GraphError> {
        let source_erased = ErasedOutputPort::new::<Out>(source);
        let target_erased = ErasedInputPort::new::<In>(target);
        self.connect_erased(&source_erased, &target_erased)
    }

    pub fn connect_constant_source<T, In>(
        &mut self,
        source: OutputPort<T>,
        target: InputPort<In>,
    ) -> Result<(), GraphError>
    where
        T: PrimitiveConstant + OutputPortValue<ChannelItem = T>,
        In: ConnectFromConstantSource<T>,
    {
        In::connect_from_constant_source(self, source, target)
    }

    fn connect_typed_into_constant_value<T>(
        &mut self,
        source: OutputPort<T>,
        target: InputPort<ConstantValue>,
    ) -> Result<(), GraphError>
    where
        T: PrimitiveConstant + OutputPortValue<ChannelItem = T>,
    {
        let bridge = self.add_internal_node(ToConstantValueNode::<T>(PhantomData), ());
        let bridge_input = ErasedInputPort::new::<T>(bridge.single_input_port());
        let bridge_output = ErasedOutputPort::new::<ConstantValue>(bridge.single_output_port());
        let source_erased = ErasedOutputPort::new::<T>(source);
        let target_erased = ErasedInputPort::new::<ConstantValue>(target);

        self.connect_erased_internal(&source_erased, &bridge_input, false)?;
        self.connect_erased_internal(&bridge_output, &target_erased, false)?;

        let from_node = self
            .node_specs
            .get(source.node_id.0)
            .map(|node| node.id().to_owned())
            .ok_or_else(|| GraphError::Validation("source node metadata did not exist".into()))?;
        let to_node = self
            .node_specs
            .get(target.node_id.0)
            .map(|node| node.id().to_owned())
            .ok_or_else(|| GraphError::Validation("target node metadata did not exist".into()))?;
        self.edges.push(GraphEdgeSnapshot {
            from_node,
            from_port: source.name,
            to_node,
            to_port: target.name,
        });

        Ok(())
    }

    /// Connect two erased ports. Used by `connect_named` and the dynamic graph builder.
    /// Compatibility is checked by `ChannelItem` TypeId rather than exact port type.
    pub fn connect_erased(
        &mut self,
        source: &ErasedOutputPort,
        target: &ErasedInputPort,
    ) -> Result<(), GraphError> {
        self.connect_erased_internal(source, target, true)
    }

    fn connect_erased_internal(
        &mut self,
        source: &ErasedOutputPort,
        target: &ErasedInputPort,
        record_edge: bool,
    ) -> Result<(), GraphError> {
        if source.channel_item_type_id != target.channel_item_type_id {
            if target.channel_item_type_id == TypeId::of::<ConstantValue>() {
                macro_rules! try_constant_bridge {
                    ($($ty:ty),* $(,)?) => {
                        $(
                            if source.channel_item_type_id == TypeId::of::<$ty>() {
                                return self.connect_erased_into_constant_value::<$ty>(source, target);
                            }
                        )*
                    };
                }

                try_constant_bridge!(
                    f32,
                    Vec<f32>,
                    f64,
                    Vec<f64>,
                    usize,
                    Vec<usize>,
                    u32,
                    Vec<u32>,
                    u64,
                    Vec<u64>,
                    i32,
                    Vec<i32>,
                    i64,
                    Vec<i64>,
                    bool,
                    Vec<bool>,
                    String,
                    Vec<String>,
                );
            }
            return Err(GraphError::Validation(format!(
                "type mismatch: output port `{}` and input port `{}` carry incompatible value types",
                source.source_port_name, target.target_port_name
            )));
        }

        let cap = source.capacity_override.unwrap_or(self.channel_capacity);
        let receiver = (source.attach)(
            &mut self.nodes,
            source.source_node_idx,
            source.source_port_name,
            cap,
        )?;

        let target_node = self
            .nodes
            .get_mut(target.target_node_idx)
            .ok_or_else(|| GraphError::Validation("target node did not exist".into()))?;

        let cardinality = target_node
            .input_schemas
            .get(target.target_port_name)
            .ok_or(GraphError::MissingInputPort {
                node: target_node.name,
                port: target.target_port_name,
            })?
            .cardinality;

        let existing_connection_count = target_node
            .inputs
            .get(target.target_port_name)
            .map(|connections| connections.len())
            .unwrap_or(0);

        match cardinality {
            PortCardinality::Single if existing_connection_count > 0 => {
                return Err(GraphError::PortAlreadyConnected {
                    node: target_node.name,
                    port: target.target_port_name,
                });
            }
            PortCardinality::Fixed(limit) if existing_connection_count >= limit => {
                return Err(GraphError::Validation(format!(
                    "input port `{}` on node `{}` accepts at most {limit} connections",
                    target.target_port_name, target_node.name
                )));
            }
            _ => {}
        }

        target_node
            .inputs
            .entry(target.target_port_name)
            .or_default()
            .push(receiver);

        if record_edge {
            let from_node = self
                .node_specs
                .get(source.source_node_idx)
                .map(|node| node.id().to_owned())
                .ok_or_else(|| {
                    GraphError::Validation("source node metadata did not exist".into())
                })?;
            let to_node = self
                .node_specs
                .get(target.target_node_idx)
                .map(|node| node.id().to_owned())
                .ok_or_else(|| {
                    GraphError::Validation("target node metadata did not exist".into())
                })?;
            self.edges.push(GraphEdgeSnapshot {
                from_node,
                from_port: source.source_port_name,
                to_node,
                to_port: target.target_port_name,
            });
        }
        Ok(())
    }

    fn connect_erased_into_constant_value<T>(
        &mut self,
        source: &ErasedOutputPort,
        target: &ErasedInputPort,
    ) -> Result<(), GraphError>
    where
        T: PrimitiveConstant + OutputPortValue<ChannelItem = T>,
    {
        let typed_source = OutputPort::<T> {
            node_id: NodeId(source.source_node_idx),
            name: source.source_port_name,
            _marker: PhantomData,
        };
        let constant_target = InputPort::<ConstantValue> {
            node_id: NodeId(target.target_node_idx),
            name: target.target_port_name,
            _marker: PhantomData,
        };

        self.connect_typed_into_constant_value(typed_source, constant_target)
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
    fn input_port(&self, _name: &str) -> Option<ErasedInputPort> {
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

    fn initialize(_runtime: &mut OutputRuntime) -> Self {}

    async fn finalize(self, _runtime: &mut OutputRuntime) -> Result<(), GraphError> {
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
