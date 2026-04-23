use enum_dispatch::enum_dispatch;
use facet::Facet;
use facet_pretty::FacetPretty;
use graphcorder::{
    NodeInputs, NodeOutputs,
    framework::{
        BuiltGraphNode, GraphError, GraphNode, GraphNodeSpec, GraphSpec, NodeDefinition,
        RegisteredNodeSpec,
    },
    static_graph,
};

#[derive(Clone, Debug, Facet)]
struct ProducerConfig {
    value: Vec<f32>,
}

#[derive(Clone, Debug, Facet)]
struct ProducerNodeSpec {
    config: ProducerConfig,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
struct ProducerOutput {
    value: Vec<f32>,
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
            value: config.value.to_owned(),
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
    value: Vec<f32>,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
struct ScaleOutput {
    result: Vec<f32>,
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
            result: input.value.into_iter().map(|v| v * config.factor).collect(),
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
    value: Vec<f32>,
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
        println!("{}: {:?}", config.label, input.value,);
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
    ) -> BuiltGraphNode<Node>;
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
    ) -> BuiltGraphNode<Node> {
        BuiltGraphNode::from_handle(builder.add(ProducerNodeSpec::new(self.config.clone())))
    }
}

impl ExampleNodeRegistry for GraphNode<ScaleConfig> {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> BuiltGraphNode<Node> {
        BuiltGraphNode::from_handle(builder.add(ScaleNodeSpec::new(self.config.clone())))
    }
}

impl ExampleNodeRegistry for GraphNode<PrintConfig> {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> BuiltGraphNode<Node> {
        BuiltGraphNode::from_handle(builder.add(PrintNodeSpec::new(self.config.clone())))
    }
}

impl RegisteredNodeSpec for Node {
    fn id(&self) -> &str {
        ExampleNodeRegistry::id(self)
    }

    fn add_to_builder(
        &self,
        builder: &mut graphcorder::framework::GraphBuilder<Node>,
    ) -> BuiltGraphNode<Node> {
        ExampleNodeRegistry::add_to_builder(self, builder)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let instance = graphcorder::init::<Node>();
    let builder = static_graph! {
        registry: Node;

        node producer: ProducerNodeSpec = ProducerNodeSpec::new(ProducerConfig {
            value: [6.0, 12.0, 18.0, 24.0].into(),
        });
        node scale_1x: ScaleNodeSpec = ScaleNodeSpec::new(ScaleConfig { factor: 1.5 });
        node scale_2x: ScaleNodeSpec = ScaleNodeSpec::new(ScaleConfig { factor: 3.0 });
        node print_1x: PrintNodeSpec = PrintNodeSpec::new(PrintConfig {
            label: "programmatic result".into(),
        });
        node print_2x: PrintNodeSpec = PrintNodeSpec::new(PrintConfig {
            label: "programmatic result2".into(),
        });

        connect producer.value -> scale_1x.value;
        connect producer.value -> scale_2x.value;
        connect scale_1x.result -> print_1x.value;
        connect scale_2x.result -> print_2x.value;
    }?;

    let spec = builder.graph_spec();
    println!("{}", spec.pretty());
    println!("{}", facet_json::to_string_pretty(&spec)?);
    println!(
        "{}",
        facet_json::to_string_pretty(&instance.graph_schema())?
    );

    builder.build().run().await?;

    let round_trip: GraphSpec<Node> = facet_json::from_str(&facet_json::to_string(&spec)?)?;
    instance.build_graph_from_spec(round_trip)?.run().await?;

    Ok(())
}
