use graphcorder::pipeline::{
    ProducerConfig, ScaleConfig, build_graph_from_json, build_programmatic_graph, example_graph_spec,
    graph_schema, graph_spec_to_rust_builder, graph_spec_to_rust_struct,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (graph, mut result) =
        build_programmatic_graph(ProducerConfig { value: 6.0 }, ScaleConfig { factor: 1.5 })?;

    let run = tokio::spawn(graph.run());
    if let Some(value) = result.recv().await {
        println!("programmatic result: {value}");
    }
    run.await??;

    let json = facet_json::to_string_pretty(&example_graph_spec())?;
    println!("{json}");
    println!("{}", graph_spec_to_rust_struct(&example_graph_spec()));
    println!("{}", graph_spec_to_rust_builder(&example_graph_spec())?);

    let (graph, mut result, _) = build_graph_from_json(&json)?;
    let run = tokio::spawn(graph.run());
    if let Some(value) = result.recv().await {
        println!("json result: {value}");
    }
    run.await??;

    println!("{}", facet_json::to_string_pretty(&graph_schema())?);
    Ok(())
}
