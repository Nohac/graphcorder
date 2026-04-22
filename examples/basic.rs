use facet_pretty::FacetPretty;
use graphcorder::{
    framework::GraphBuilder,
    pipeline::{
        GraphBuilderGraphSpecExt, GraphSpec, PrintConfig, PrintNodeSpec, ProducerConfig,
        ProducerNodeSpec, ScaleConfig, ScaleNodeSpec, build_graph_from_spec,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = GraphBuilder::new();

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

    let spec = builder.graph_spec()?;
    println!("{}", spec.pretty());

    let graph = builder.build();
    graph.run().await?;

    let json_spec = facet_json::to_string_pretty(&spec)?;
    println!("{json_spec}");

    let graph_spec: GraphSpec = facet_json::from_str(&json_spec)?;
    let graph = build_graph_from_spec(graph_spec)?;
    graph.run().await?;

    Ok(())
}
