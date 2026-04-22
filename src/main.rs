use graphcorder::pipeline::{
    ProducerConfig, ScaleConfig, build_graph_from_json, build_programmatic_graph, graph_schema,
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

    let json = serde_json::json!({
        "nodes": [
            { "kind": "producer", "id": "producer_1", "config": { "value": 6.0 } },
            { "kind": "scale", "id": "scale_1", "config": { "factor": 1.5 } }
        ],
        "edges": [
            {
                "from": { "node": "producer_1", "port": "value" },
                "to": { "node": "scale_1", "port": "value" }
            }
        ]
    })
    .to_string();

    let (graph, mut result, _) = build_graph_from_json(&json)?;
    println!("{:?}", graph);
    let run = tokio::spawn(graph.run());
    if let Some(value) = result.recv().await {
        println!("json result: {value}");
    }
    run.await??;

    println!("{}", serde_json::to_string_pretty(&graph_schema())?);
    Ok(())
}
