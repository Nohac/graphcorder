use std::collections::BTreeMap;

use schemars::{JsonSchema, Schema, schema_for};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::graph::{GraphBuilder, GraphError, InputPort, NodeContext, NodeFuture, NodeHandle, OutputPort};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProducerConfig {
    pub value: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScaleConfig {
    pub factor: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProducerOutputSchema {
    pub value: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScaleInputSchema {
    pub value: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScaleOutputSchema {
    pub result: f32,
}

pub struct ProducerPorts {
    pub value: OutputPort<f32>,
}

pub struct ScalePorts {
    pub value: InputPort<f32>,
    pub result: OutputPort<f32>,
}

pub fn add_producer(builder: &mut GraphBuilder, config: ProducerConfig) -> NodeHandle<ProducerPorts> {
    builder.add_node(
        "producer",
        |node_id| ProducerPorts {
            value: OutputPort::new(node_id, "value"),
        },
        move |ctx| producer_task(config, ctx),
    )
}

pub fn add_scale(builder: &mut GraphBuilder, config: ScaleConfig) -> NodeHandle<ScalePorts> {
    builder.add_node(
        "scale",
        |node_id| ScalePorts {
            value: InputPort::new(node_id, "value"),
            result: OutputPort::new(node_id, "result"),
        },
        move |ctx| scale_task(config, ctx),
    )
}

fn producer_task(config: ProducerConfig, mut ctx: NodeContext) -> NodeFuture {
    let sender = ctx.take_output::<f32>("value");

    Box::pin(async move {
        let sender = sender.map_err(wrap_pipeline_error)?;
        sender.send(config.value).await.map_err(|_| GraphError::NodeExecution {
            node: "producer",
            message: "downstream receiver closed".into(),
        })?;
        Ok(())
    })
}

fn scale_task(config: ScaleConfig, mut ctx: NodeContext) -> NodeFuture {
    let input = ctx.take_input::<f32>("value");
    let output = ctx.take_output::<f32>("result");

    Box::pin(async move {
        let mut input = input.map_err(wrap_pipeline_error)?;
        let output = output.map_err(wrap_pipeline_error)?;

        while let Some(value) = input.recv().await {
            output
                .send(value * config.factor)
                .await
                .map_err(|_| GraphError::NodeExecution {
                    node: "scale",
                    message: "result receiver closed".into(),
                })?;
        }

        Ok(())
    })
}

fn wrap_pipeline_error(error: GraphError) -> GraphError {
    GraphError::NodeExecution {
        node: "pipeline",
        message: error.to_string(),
    }
}

pub fn build_programmatic_graph(
    producer: ProducerConfig,
    scale: ScaleConfig,
) -> Result<(GraphBuilder, mpsc::Receiver<f32>), GraphError> {
    let mut builder = GraphBuilder::new();
    let producer = add_producer(&mut builder, producer);
    let scale = add_scale(&mut builder, scale);

    builder.connect(producer.ports.value, scale.ports.value)?;
    let result = builder.capture_output(scale.ports.result)?;
    Ok((builder, result))
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
    Producer(NodeHandle<ProducerPorts>),
    Scale(NodeHandle<ScalePorts>),
}

pub fn build_graph_from_json(
    json: &str,
) -> Result<(GraphBuilder, mpsc::Receiver<f32>, GraphSpec), GraphError> {
    let spec: GraphSpec = serde_json::from_str(json)
        .map_err(|error| GraphError::Validation(format!("invalid graph json: {error}")))?;
    let mut builder = GraphBuilder::new();
    let mut nodes = BTreeMap::new();

    for node in &spec.nodes {
        match node {
            NodeSpec::Producer { id, config } => {
                nodes.insert(id.clone(), BuiltNode::Producer(add_producer(&mut builder, config.clone())));
            }
            NodeSpec::Scale { id, config } => {
                nodes.insert(id.clone(), BuiltNode::Scale(add_scale(&mut builder, config.clone())));
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
                builder.connect(source.ports.value, target.ports.value)?;
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

    let result = builder.capture_output(scale.ports.result)?;
    Ok((builder, result, spec))
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
