use facet_pretty::FacetPretty;
use graphcorder::{
    framework::GraphBuilder,
    pipeline::{
        GraphBuilderGraphSpecExt, GraphSpec, ProducerConfig, ProducerNodeSpec, ScaleConfig,
        ScaleNodeSpec, build_graph_from_spec,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer_conf = ProducerConfig { value: 6.0 };
    let scale_conf = ScaleConfig { factor: 1.5 };

    let mut builder = GraphBuilder::new();
    let producer = builder.add(ProducerNodeSpec::new(producer_conf));
    let scale_1x = builder.add(ScaleNodeSpec::new(scale_conf.clone()));
    let scale_2x = builder.add(ScaleNodeSpec::new(ScaleConfig {
        factor: scale_conf.factor * 2.0,
    }));

    builder.connect(producer.output.value, scale_1x.input.value)?;
    builder.connect(producer.output.value, scale_2x.input.value)?;
    let mut result = builder.capture_output(scale_1x.output.result)?;
    let mut result2 = builder.capture_output(scale_2x.output.result)?;

    let spec = builder.graph_spec().unwrap();

    println!("{}", spec.pretty());

    let graph = builder.build();

    let run = tokio::spawn(graph.run());
    if let Some(value) = result.recv().await {
        println!("programmatic result: {value}");
    }
    if let Some(value) = result2.recv().await {
        println!("programmatic result2: {value}");
    }
    run.await??;

    let json_spec = facet_json::to_string_pretty(&spec).unwrap();
    println!("{}", json_spec);
    let graph: GraphSpec = facet_json::from_str(&json_spec).unwrap();

    let (graph, mut result, _) = build_graph_from_spec(graph)?;
    let run = tokio::spawn(graph.run());
    if let Some(value) = result.recv().await {
        println!("json result: {value}");
    }
    run.await??;

    Ok(())
}
