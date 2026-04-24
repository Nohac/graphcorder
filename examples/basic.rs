use facet::Facet;
use facet_pretty::FacetPretty;
use graphcorder::{
    GraphNode, NodeInputs, NodeOutputs, NodeRegistry,
    framework::{GraphError, GraphSpec, NodeDefinition},
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
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        output.value = config.value.to_owned();
        Ok(())
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
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        output.result = input.value.into_iter().map(|v| v * config.factor).collect();
        Ok(())
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
        _output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        println!("{}: {:?}", config.label, input.value,);
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Debug, Facet, NodeRegistry)]
enum Node {
    Constant(graphcorder::framework::ConstantGraphNode),
    Producer(ProducerGraphNode),
    Scale(ScaleGraphNode),
    Print(PrintGraphNode),
    PrintScalar(PrintScalarGraphNode),
}

#[derive(Clone, Debug, Facet)]
struct PrintScalarConfig {
    label: String,
}

#[derive(GraphNode)]
struct PrintScalarNode;

impl NodeDefinition for PrintScalarNode {
    type Config = PrintScalarConfig;
    type Input = f32;
    type Output = ();

    async fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
        _output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        println!("{}: {}", config.label, input);
        Ok(())
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
        node print_scalar = PrintScalarNode {
            label: "constant result".into(),
        };
        node constant_number = 3.0f32;

        connect producer -> [scale_1x, scale_2x];
        connect scale_1x -> print_1x;
        connect scale_2x -> print_2x;
        connect constant_number -> print_scalar;
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
