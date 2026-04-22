use enum_dispatch::enum_dispatch;
use facet::Facet;
use facet_pretty::FacetPretty;
use graphcorder::{
    NodeInputs, NodeOutputs,
    framework::{
        BuiltNode, GraphError, GraphNode, GraphNodeSpec, GraphSpec, NodeDefinition, NodeHandle,
        RegisteredNodeSpec,
    },
};

#[derive(Clone, Debug, Facet)]
struct ProducerConfig {
    value: f32,
}

#[derive(Clone, Debug, Facet)]
struct ProducerNodeSpec {
    config: ProducerConfig,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
struct ProducerOutput {
    value: f32,
}

struct ProducerNode;

impl ProducerNodeSpec {
    fn new(config: ProducerConfig) -> Self {
        Self { config }
    }
}

impl NodeDefinition for ProducerNode {
    type Config = ProducerConfig;
    type Input = ();
    type Output = ProducerOutput;

    const KIND: &'static str = "producer";

    async fn run(
        &self,
        _input: Self::Input,
        config: &Self::Config,
    ) -> Result<Self::Output, GraphError> {
        Ok(ProducerOutput {
            value: config.value,
        })
    }
}

impl GraphNodeSpec for ProducerNodeSpec {
    type Node = ProducerNode;
    type Registry = Node;

    fn kind(&self) -> &'static str {
        ProducerNode::KIND
    }

    fn export_node(&self, id: String) -> Self::Registry {
        Node::Producer(GraphNode::new(id, self.config.clone()))
    }

    fn into_parts(self) -> (Self::Node, ProducerConfig) {
        (ProducerNode, self.config)
    }
}

#[derive(Clone, Debug, Facet)]
struct ScaleConfig {
    factor: f32,
}

#[derive(Clone, Debug, Facet)]
struct ScaleNodeSpec {
    config: ScaleConfig,
}

#[derive(Clone, Debug, Facet, NodeInputs)]
struct ScaleInput {
    value: f32,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
struct ScaleOutput {
    result: f32,
}

struct ScaleNode;

impl ScaleNodeSpec {
    fn new(config: ScaleConfig) -> Self {
        Self { config }
    }
}

impl NodeDefinition for ScaleNode {
    type Config = ScaleConfig;
    type Input = ScaleInput;
    type Output = ScaleOutput;

    const KIND: &'static str = "scale";

    async fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
    ) -> Result<Self::Output, GraphError> {
        Ok(ScaleOutput {
            result: input.value * config.factor,
        })
    }
}

impl GraphNodeSpec for ScaleNodeSpec {
    type Node = ScaleNode;
    type Registry = Node;

    fn kind(&self) -> &'static str {
        ScaleNode::KIND
    }

    fn export_node(&self, id: String) -> Self::Registry {
        Node::Scale(GraphNode::new(id, self.config.clone()))
    }

    fn into_parts(self) -> (Self::Node, ScaleConfig) {
        (ScaleNode, self.config)
    }
}

#[derive(Clone, Debug, Facet)]
struct PrintConfig {
    label: String,
}

#[derive(Clone, Debug, Facet)]
struct PrintNodeSpec {
    config: PrintConfig,
}

#[derive(Clone, Debug, Facet, NodeInputs)]
struct PrintInput {
    value: f32,
}

struct PrintNode;

impl PrintNodeSpec {
    fn new(config: PrintConfig) -> Self {
        Self { config }
    }
}

impl NodeDefinition for PrintNode {
    type Config = PrintConfig;
    type Input = PrintInput;
    type Output = ();

    const KIND: &'static str = "print";

    async fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
    ) -> Result<Self::Output, GraphError> {
        println!("{}: {}", config.label, input.value);
        Ok(())
    }
}

impl GraphNodeSpec for PrintNodeSpec {
    type Node = PrintNode;
    type Registry = Node;

    fn kind(&self) -> &'static str {
        PrintNode::KIND
    }

    fn export_node(&self, id: String) -> Self::Registry {
        Node::Print(GraphNode::new(id, self.config.clone()))
    }

    fn into_parts(self) -> (Self::Node, PrintConfig) {
        (PrintNode, self.config)
    }
}

#[enum_dispatch]
trait ExampleNodeRegistry {
    fn id(&self) -> &str;
    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> BuiltNodeHandle;
}

#[repr(C)]
#[derive(Clone, Debug, Facet)]
#[enum_dispatch(ExampleNodeRegistry)]
enum Node {
    Producer(GraphNode<ProducerConfig>),
    Scale(GraphNode<ScaleConfig>),
    Print(GraphNode<PrintConfig>),
}

impl ExampleNodeRegistry for GraphNode<ProducerConfig> {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> BuiltNodeHandle {
        BuiltNodeHandle::Producer(builder.add(ProducerNodeSpec::new(self.config.clone())))
    }
}

impl ExampleNodeRegistry for GraphNode<ScaleConfig> {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> BuiltNodeHandle {
        BuiltNodeHandle::Scale(builder.add(ScaleNodeSpec::new(self.config.clone())))
    }
}

impl ExampleNodeRegistry for GraphNode<PrintConfig> {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> BuiltNodeHandle {
        BuiltNodeHandle::Print(builder.add(PrintNodeSpec::new(self.config.clone())))
    }
}

enum BuiltNodeHandle {
    Producer(NodeHandle<ProducerNode>),
    Scale(NodeHandle<ScaleNode>),
    Print(NodeHandle<PrintNode>),
}

impl BuiltNode<Node> for BuiltNodeHandle {
    fn connect_to(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
        from_port: &str,
        target: &BuiltNodeHandle,
        to_port: &str,
    ) -> Result<(), GraphError> {
        match (self, from_port, target, to_port) {
            (BuiltNodeHandle::Producer(source), "value", BuiltNodeHandle::Scale(target), "value") => {
                builder.connect(source.output.value, target.input.value)
            }
            (BuiltNodeHandle::Scale(source), "result", BuiltNodeHandle::Print(target), "value") => {
                builder.connect(source.output.result, target.input.value)
            }
            _ => Err(GraphError::Validation(format!(
                "unsupported edge {} -> {}",
                from_port, to_port
            ))),
        }
    }
}

impl RegisteredNodeSpec for Node {
    type BuiltNode = BuiltNodeHandle;

    fn id(&self) -> &str {
        ExampleNodeRegistry::id(self)
    }

    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> Self::BuiltNode {
        ExampleNodeRegistry::add_to_builder(self, builder)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let instance = graphcorder::init::<Node>();
    let mut builder = instance.builder();

    let producer = builder.add(ProducerNodeSpec::new(ProducerConfig { value: 6.0 }));
    let scale_1x = builder.add(ScaleNodeSpec::new(ScaleConfig { factor: 1.5 }));
    let scale_2x = builder.add(ScaleNodeSpec::new(ScaleConfig { factor: 3.0 }));
    let print_1x = builder.add(PrintNodeSpec::new(PrintConfig {
        label: "programmatic result".into(),
    }));
    let print_2x = builder.add(PrintNodeSpec::new(PrintConfig {
        label: "programmatic result2".into(),
    }));

    builder.connect(producer.output.value, scale_1x.input.value)?;
    builder.connect(producer.output.value, scale_2x.input.value)?;
    builder.connect(scale_1x.output.result, print_1x.input.value)?;
    builder.connect(scale_2x.output.result, print_2x.input.value)?;

    let spec = builder.graph_spec();
    println!("{}", spec.pretty());
    println!("{}", facet_json::to_string_pretty(&spec)?);
    println!("{}", facet_json::to_string_pretty(&instance.graph_schema())?);

    builder.build().run().await?;

    let round_trip: GraphSpec<Node> = facet_json::from_str(&facet_json::to_string(&spec)?)?;
    instance.build_graph_from_spec(round_trip)?.run().await?;

    Ok(())
}
