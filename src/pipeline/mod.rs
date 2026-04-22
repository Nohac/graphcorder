use std::collections::BTreeMap;
use std::fmt::Write;

use facet::Facet;
use facet_json::{from_str, to_string};
use facet_json_schema::{JsonSchema, schema_for};
use facet_pretty::FacetPretty;
use tokio::sync::mpsc;

use crate::framework::{Graph, GraphBuilder, GraphError};
pub use crate::nodes::producer::ProducerConfig;
pub use crate::nodes::producer::ProducerNodeSpec;
pub use crate::nodes::scale::ScaleConfig;
pub use crate::nodes::scale::ScaleNodeSpec;

pub fn build_programmatic_graph(
    producer: ProducerConfig,
    scale: ScaleConfig,
) -> Result<(Graph, mpsc::Receiver<f32>, mpsc::Receiver<f32>), GraphError> {
    let mut builder = GraphBuilder::new();
    let producer = builder.add(ProducerNodeSpec::new(producer));
    let scale_1x = builder.add(ScaleNodeSpec::new(scale.clone()));
    let scale_2x = builder.add(ScaleNodeSpec::new(ScaleConfig {
        factor: scale.factor * 2.0,
    }));

    builder.connect(producer.output.value, scale_1x.input.value)?;
    builder.connect(producer.output.value, scale_2x.input.value)?;
    let result = builder.capture_output(scale_1x.output.result)?;
    let result2 = builder.capture_output(scale_2x.output.result)?;
    Ok((builder.build(), result, result2))
}

#[derive(Clone, Debug, Facet)]
pub struct GraphSpec {
    pub nodes: Vec<NodeSpec>,
    pub edges: Vec<EdgeSpec>,
}

#[repr(C)]
#[derive(Clone, Debug, Facet)]
pub enum NodeSpec {
    Producer(ExportedNode<ProducerConfig>),
    Scale(ExportedNode<ScaleConfig>),
}

#[derive(Clone, Debug, Facet)]
pub struct EdgeSpec {
    pub from: PortRef,
    pub to: PortRef,
}

#[derive(Clone, Debug, Facet)]
pub struct PortRef {
    pub node: String,
    pub port: String,
}

#[derive(Clone, Debug, Facet)]
pub struct ExportedNode<Config> {
    pub id: String,
    pub config: Config,
}

pub trait GraphBuilderGraphSpecExt {
    fn graph_spec(&self) -> Result<GraphSpec, GraphError>;
}

impl GraphBuilderGraphSpecExt for GraphBuilder {
    fn graph_spec(&self) -> Result<GraphSpec, GraphError> {
        let snapshot = self.spec_snapshot();
        let mut nodes = Vec::with_capacity(snapshot.nodes.len());

        for node in snapshot.nodes {
            match node.kind {
                "producer" => nodes.push(NodeSpec::Producer(ExportedNode {
                    id: node.id,
                    config: from_str(&node.config_json).map_err(|error| {
                        GraphError::Validation(format!(
                            "invalid stored producer config json: {error}"
                        ))
                    })?,
                })),
                "scale" => nodes.push(NodeSpec::Scale(ExportedNode {
                    id: node.id,
                    config: from_str(&node.config_json).map_err(|error| {
                        GraphError::Validation(format!(
                            "invalid stored scale config json: {error}"
                        ))
                    })?,
                })),
                other => {
                    return Err(GraphError::Validation(format!(
                        "unsupported node kind `{other}` in builder snapshot"
                    )));
                }
            }
        }

        let edges = snapshot
            .edges
            .into_iter()
            .map(|edge| EdgeSpec {
                from: PortRef {
                    node: edge.from_node,
                    port: edge.from_port.into(),
                },
                to: PortRef {
                    node: edge.to_node,
                    port: edge.to_port.into(),
                },
            })
            .collect();

        Ok(GraphSpec { nodes, edges })
    }
}

enum BuiltNode {
    Producer(crate::framework::NodeHandle<crate::nodes::producer::ProducerNode>),
    Scale(crate::framework::NodeHandle<crate::nodes::scale::ScaleNode>),
}

pub fn build_graph_from_json(
    json: &str,
) -> Result<(Graph, mpsc::Receiver<f32>, GraphSpec), GraphError> {
    let spec: GraphSpec = from_str(json)
        .map_err(|error| GraphError::Validation(format!("invalid graph json: {error}")))?;
    let mut builder = GraphBuilder::new();
    let mut nodes = BTreeMap::new();

    for node in &spec.nodes {
        match node {
            NodeSpec::Producer(spec) => {
                nodes.insert(
                    spec.id.clone(),
                    BuiltNode::Producer(builder.add(ProducerNodeSpec::new(spec.config.clone()))),
                );
            }
            NodeSpec::Scale(spec) => {
                nodes.insert(
                    spec.id.clone(),
                    BuiltNode::Scale(builder.add(ScaleNodeSpec::new(spec.config.clone()))),
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
            (
                Some(BuiltNode::Producer(source)),
                Some(BuiltNode::Scale(target)),
                "value",
                "value",
            ) => {
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

pub fn graph_schema() -> JsonSchema {
    schema_for::<GraphSpec>()
}

pub fn example_graph_spec() -> GraphSpec {
    GraphSpec {
        nodes: vec![
            NodeSpec::Producer(ExportedNode {
                id: "producer_1".into(),
                config: ProducerConfig { value: 6.0 },
            }),
            NodeSpec::Scale(ExportedNode {
                id: "scale_1".into(),
                config: ScaleConfig { factor: 1.5 },
            }),
        ],
        edges: vec![EdgeSpec {
            from: PortRef {
                node: "producer_1".into(),
                port: "value".into(),
            },
            to: PortRef {
                node: "scale_1".into(),
                port: "value".into(),
            },
        }],
    }
}

pub fn graph_spec_to_json(spec: &GraphSpec) -> Result<String, GraphError> {
    to_string(spec)
        .map_err(|error| GraphError::Validation(format!("could not encode graph json: {error}")))
}

pub fn graph_spec_to_rust_struct(spec: &GraphSpec) -> String {
    spec.pretty().to_string()
}

pub fn graph_spec_to_rust_builder(spec: &GraphSpec) -> Result<String, GraphError> {
    let mut code = String::new();
    writeln!(&mut code, "let mut builder = GraphBuilder::new();").expect("write to string");

    for node in &spec.nodes {
        match node {
            NodeSpec::Producer(spec) => {
                writeln!(
                    &mut code,
                    "let {} = builder.add(ProducerNodeSpec::new(ProducerConfig {{ value: {:?} }}));",
                    sanitize_identifier(&spec.id),
                    spec.config.value
                )
                .expect("write to string");
            }
            NodeSpec::Scale(spec) => {
                writeln!(
                    &mut code,
                    "let {} = builder.add(ScaleNodeSpec::new(ScaleConfig {{ factor: {:?} }}));",
                    sanitize_identifier(&spec.id),
                    spec.config.factor
                )
                .expect("write to string");
            }
        }
    }

    for edge in &spec.edges {
        let from = sanitize_identifier(&edge.from.node);
        let to = sanitize_identifier(&edge.to.node);
        let line = match (edge.from.port.as_str(), edge.to.port.as_str()) {
            ("value", "value") => {
                format!("builder.connect({from}.output.value, {to}.input.value)?;")
            }
            _ => {
                return Err(GraphError::Validation(format!(
                    "unsupported edge {}.{} -> {}.{}",
                    edge.from.node, edge.from.port, edge.to.node, edge.to.port
                )));
            }
        };
        writeln!(&mut code, "{line}").expect("write to string");
    }

    if let Some(scale_id) = spec.nodes.iter().find_map(|node| match node {
        NodeSpec::Scale(spec) => Some(sanitize_identifier(&spec.id)),
        NodeSpec::Producer(_) => None,
    }) {
        writeln!(
            &mut code,
            "let result = builder.capture_output({scale_id}.output.result)?;"
        )
        .expect("write to string");
    }

    writeln!(&mut code, "let graph = builder.build();").expect("write to string");
    Ok(code)
}

fn sanitize_identifier(id: &str) -> String {
    let mut sanitized = String::with_capacity(id.len());

    for (index, ch) in id.chars().enumerate() {
        if (index == 0 && (ch.is_ascii_alphabetic() || ch == '_'))
            || (index > 0 && (ch.is_ascii_alphanumeric() || ch == '_'))
        {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    if sanitized.is_empty() {
        "_node".into()
    } else if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        format!("_{sanitized}")
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn programmatic_graph_runs() {
        let (graph, mut result, mut result2) =
            build_programmatic_graph(ProducerConfig { value: 3.5 }, ScaleConfig { factor: 2.0 })
                .expect("graph builds");

        let run = tokio::spawn(graph.run());
        let value = result.recv().await.expect("result value");
        let value2 = result2.recv().await.expect("result value");
        assert_eq!(value, 7.0);
        assert_eq!(value2, 14.0);
        run.await.expect("task join").expect("graph run");
    }

    #[tokio::test]
    async fn json_graph_runs() {
        let json = graph_spec_to_json(&GraphSpec {
            nodes: vec![
                NodeSpec::Producer(ExportedNode {
                    id: "producer_1".into(),
                    config: ProducerConfig { value: 4.0 },
                }),
                NodeSpec::Scale(ExportedNode {
                    id: "scale_1".into(),
                    config: ScaleConfig { factor: 0.5 },
                }),
            ],
            edges: vec![EdgeSpec {
                from: PortRef {
                    node: "producer_1".into(),
                    port: "value".into(),
                },
                to: PortRef {
                    node: "scale_1".into(),
                    port: "value".into(),
                },
            }],
        })
        .expect("graph json");

        let (graph, mut result, _) = build_graph_from_json(&json).expect("graph loads");

        let run = tokio::spawn(graph.run());
        let value = result.recv().await.expect("result value");
        assert_eq!(value, 2.0);
        run.await.expect("task join").expect("graph run");
    }

    #[test]
    fn rust_exports_render() {
        let spec = example_graph_spec();

        let pretty = graph_spec_to_rust_struct(&spec);
        assert!(pretty.contains("GraphSpec"));
        assert!(pretty.contains("Producer"));

        let builder = graph_spec_to_rust_builder(&spec).expect("builder code");
        assert!(builder.contains("GraphBuilder::new()"));
        assert!(builder.contains("builder.connect("));
        assert!(builder.contains("ProducerNode"));
        assert!(builder.contains("ScaleNode"));
    }

    #[test]
    fn builder_assigns_readable_ids() {
        let mut builder = GraphBuilder::new();
        let producer = builder.add(ProducerNodeSpec::new(ProducerConfig { value: 1.0 }));
        let scale = builder.add(ScaleNodeSpec::new(ScaleConfig { factor: 2.0 }));

        assert_eq!(producer.id, "producer_1");
        assert_eq!(scale.id, "scale_1");
    }

    #[test]
    fn builder_can_export_graph_spec() {
        let mut builder = GraphBuilder::new();
        let producer = builder.add(ProducerNodeSpec::new(ProducerConfig { value: 6.0 }));
        let scale = builder.add(ScaleNodeSpec::new(ScaleConfig { factor: 1.5 }));

        builder
            .connect(producer.output.value, scale.input.value)
            .expect("connect");

        let spec = builder.graph_spec().expect("graph spec");
        assert_eq!(spec.nodes.len(), 2);
        assert_eq!(spec.edges.len(), 1);
        assert!(matches!(&spec.nodes[0], NodeSpec::Producer(_)));
        assert_eq!(spec.edges[0].from.node, "producer_1");
        assert_eq!(spec.edges[0].to.node, "scale_1");
    }

    #[tokio::test]
    async fn one_output_can_feed_multiple_inputs() {
        let mut builder = GraphBuilder::new();
        let producer = builder.add(ProducerNodeSpec::new(ProducerConfig { value: 5.0 }));
        let scale_1 = builder.add(ScaleNodeSpec::new(ScaleConfig { factor: 2.0 }));
        let scale_2 = builder.add(ScaleNodeSpec::new(ScaleConfig { factor: 3.0 }));

        builder.connect(producer.output.value, scale_1.input.value).expect("connect scale_1");
        builder.connect(producer.output.value, scale_2.input.value).expect("connect scale_2");
        let mut result_1 = builder.capture_output(scale_1.output.result).expect("capture scale_1");
        let mut result_2 = builder.capture_output(scale_2.output.result).expect("capture scale_2");

        let graph = builder.build();
        let run = tokio::spawn(graph.run());

        assert_eq!(result_1.recv().await.expect("scale_1 value"), 10.0);
        assert_eq!(result_2.recv().await.expect("scale_2 value"), 15.0);
        run.await.expect("task join").expect("graph run");
    }
}
