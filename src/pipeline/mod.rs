use std::collections::BTreeMap;

use schemars::{JsonSchema, Schema, schema_for};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::framework::{Graph, GraphBuilder, GraphError};
use crate::nodes::producer::ProducerNode;
use crate::nodes::scale::ScaleNode;

pub use crate::nodes::producer::ProducerConfig;
pub use crate::nodes::scale::ScaleConfig;

pub fn build_programmatic_graph(
    producer: ProducerConfig,
    scale: ScaleConfig,
) -> Result<(Graph, mpsc::Receiver<f32>), GraphError> {
    let mut builder = GraphBuilder::new();
    let producer = builder.add_node(ProducerNode, producer);
    let scale = builder.add_node(ScaleNode, scale);

    builder.connect(producer.output.value, scale.input.value)?;
    let result = builder.capture_output(scale.output.result)?;
    Ok((builder.build(), result))
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct GraphSpec {
    pub nodes: Vec<NodeSpec>,
    pub edges: Vec<EdgeSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeSpec {
    Producer { id: String, config: ProducerConfig },
    Scale { id: String, config: ScaleConfig },
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct EdgeSpec {
    pub from: PortRef,
    pub to: PortRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct PortRef {
    pub node: String,
    pub port: String,
}

enum BuiltNode {
    Producer(crate::framework::NodeHandle<ProducerNode>),
    Scale(crate::framework::NodeHandle<ScaleNode>),
}

pub fn build_graph_from_json(
    json: &str,
) -> Result<(Graph, mpsc::Receiver<f32>, GraphSpec), GraphError> {
    let spec: GraphSpec = serde_json::from_str(json)
        .map_err(|error| GraphError::Validation(format!("invalid graph json: {error}")))?;
    let mut builder = GraphBuilder::new();
    let mut nodes = BTreeMap::new();

    for node in &spec.nodes {
        match node {
            NodeSpec::Producer { id, config } => {
                nodes.insert(
                    id.clone(),
                    BuiltNode::Producer(builder.add_node(ProducerNode, config.clone())),
                );
            }
            NodeSpec::Scale { id, config } => {
                nodes.insert(
                    id.clone(),
                    BuiltNode::Scale(builder.add_node(ScaleNode, config.clone())),
                );
            }
        }
    }

    for edge in &spec.edges {
        match (
            nodes.get(&edge.from.node),
            nodes.get(&edge.to.node),
            edge.from.port.as_str(),
            edge.to.port.as_str(),
        ) {
            (Some(BuiltNode::Producer(source)), Some(BuiltNode::Scale(target)), "value", "value") => {
                builder.connect(source.output.value, target.input.value)?;
            }
            _ => {
                return Err(GraphError::Validation(format!(
                    "unsupported edge {}.{} -> {}.{}",
                    edge.from.node, edge.from.port, edge.to.node, edge.to.port
                )));
            }
        }
    }

    let scale = nodes
        .values()
        .find_map(|node| match node {
            BuiltNode::Scale(handle) => Some(handle),
            BuiltNode::Producer(_) => None,
        })
        .ok_or_else(|| GraphError::Validation("graph did not contain a scale node".into()))?;

    let result = builder.capture_output(scale.output.result)?;
    Ok((builder.build(), result, spec))
}

pub fn graph_schema() -> Schema {
    schema_for!(GraphSpec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn programmatic_graph_runs() {
        let (graph, mut result) = build_programmatic_graph(
            ProducerConfig { value: 3.5 },
            ScaleConfig { factor: 2.0 },
        )
        .expect("graph builds");

        let run = tokio::spawn(graph.run());
        let value = result.recv().await.expect("result value");
        assert_eq!(value, 7.0);
        run.await.expect("task join").expect("graph run");
    }

    #[tokio::test]
    async fn json_graph_runs() {
        let json = serde_json::json!({
            "nodes": [
                { "kind": "producer", "id": "producer_1", "config": { "value": 4.0 } },
                { "kind": "scale", "id": "scale_1", "config": { "factor": 0.5 } }
            ],
            "edges": [
                {
                    "from": { "node": "producer_1", "port": "value" },
                    "to": { "node": "scale_1", "port": "value" }
                }
            ]
        })
        .to_string();

        let (graph, mut result, _) = build_graph_from_json(&json).expect("graph loads");

        let run = tokio::spawn(graph.run());
        let value = result.recv().await.expect("result value");
        assert_eq!(value, 2.0);
        run.await.expect("task join").expect("graph run");
    }
}
