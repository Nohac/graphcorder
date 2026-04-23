use enum_dispatch::enum_dispatch;
use facet::Facet;
use facet_pretty::FacetPretty;
use graphcorder::{
    GraphNode, NodeInputs, NodeOutputs,
    framework::{
        BuiltGraphNode, GraphError, GraphNode as ExportedGraphNode, GraphSpec, NodeDefinition,
        RegisteredNodeSpec,
    },
    static_graph,
};

#[derive(Clone, Debug, Facet)]
struct ProducerConfig {
    value: Vec<f32>,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
struct ProducerOutput {
    value: Vec<f32>,
}

#[derive(GraphNode)]
struct ProducerNode;

impl NodeDefinition for ProducerNode {
    type Config = ProducerConfig;
    type Input = ();
    type Output = ProducerOutput;

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

#[derive(Clone, Debug, Facet)]
struct ScaleConfig {
    factor: f32,
}

#[derive(Clone, Debug, Facet, NodeInputs)]
struct ScaleInput {
    value: Vec<f32>,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
struct ScaleOutput {
    result: Vec<f32>,
}

#[derive(GraphNode)]
struct ScaleNode;

impl NodeDefinition for ScaleNode {
    type Config = ScaleConfig;
    type Input = ScaleInput;
    type Output = ScaleOutput;

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

#[derive(Clone, Debug, Facet)]
struct PrintConfig {
    label: String,
}

#[derive(Clone, Debug, Facet, NodeInputs)]
struct PrintInput {
    value: Vec<f32>,
}

#[derive(GraphNode)]
struct PrintNode;

impl NodeDefinition for PrintNode {
    type Config = PrintConfig;
    type Input = PrintInput;
    type Output = ();

    async fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
    ) -> Result<Self::Output, GraphError> {
        println!("{}: {:?}", config.label, input.value,);
        Ok(())
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
    Producer(ExportedGraphNode<ProducerConfig>),
    Scale(ExportedGraphNode<ScaleConfig>),
    Print(ExportedGraphNode<PrintConfig>),
}

impl ExampleNodeRegistry for ExportedGraphNode<ProducerConfig> {
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

impl ExampleNodeRegistry for ExportedGraphNode<ScaleConfig> {
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

impl ExampleNodeRegistry for ExportedGraphNode<PrintConfig> {
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

        node producer = ProducerNode {
            value: vec![6.0, 12.0, 18.0, 24.0],
        };
        node scale_1x = ScaleNode { factor: 1.5 };
        node scale_2x = ScaleNode { factor: 3.0 };
        node print_1x = PrintNode {
            label: "programmatic result".into(),
        };
        node print_2x = PrintNode {
            label: "programmatic result2".into(),
        };

        connect producer -> [scale_1x, scale_2x];
        connect scale_1x -> print_1x;
        connect scale_2x -> print_2x;
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
