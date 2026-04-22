use std::collections::BTreeMap;
use std::fmt::Write;

use enum_dispatch::enum_dispatch;
use facet::Facet;
use facet_json::to_string;
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
    println!("{}", builder.graph_spec().unwrap().pretty());
    Ok((builder.build(), result, result2))
}

#[derive(Clone, Debug, Facet)]
pub struct GraphSpec {
    pub nodes: Vec<NodeSpec>,
    pub edges: Vec<EdgeSpec>,
}

#[enum_dispatch]
pub(crate) trait RegisteredNodeSpec {
    fn id(&self) -> &str;
    fn kind(&self) -> &'static str;
    fn add_to_builder(&self, builder: &mut GraphBuilder) -> BuiltNode;
    fn write_rust_builder_stmt(&self, code: &mut String) -> Result<(), GraphError>;
}

#[repr(C)]
#[derive(Clone, Debug, Facet)]
#[enum_dispatch(RegisteredNodeSpec)]
pub enum NodeSpec {
    Producer(ProducerGraphNode),
    Scale(ScaleGraphNode),
}

impl NodeSpec {
    pub fn id(&self) -> &str {
        RegisteredNodeSpec::id(self)
    }

    pub fn kind(&self) -> &'static str {
        RegisteredNodeSpec::kind(self)
    }
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
pub struct ProducerGraphNode {
    pub id: String,
    pub config: ProducerConfig,
}

#[derive(Clone, Debug, Facet)]
pub struct ScaleGraphNode {
    pub id: String,
    pub config: ScaleConfig,
}

pub trait GraphBuilderGraphSpecExt {
    fn graph_spec(&self) -> Result<GraphSpec, GraphError>;
}

impl GraphBuilderGraphSpecExt for GraphBuilder {
    fn graph_spec(&self) -> Result<GraphSpec, GraphError> {
        let nodes = self.export_nodes().to_vec();
        let edges = self
            .edges()
            .iter()
            .cloned()
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

impl RegisteredNodeSpec for ProducerGraphNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &'static str {
        "producer"
    }

    fn add_to_builder(&self, builder: &mut GraphBuilder) -> BuiltNode {
        BuiltNode::Producer(builder.add(ProducerNodeSpec::new(self.config.clone())))
    }

    fn write_rust_builder_stmt(&self, code: &mut String) -> Result<(), GraphError> {
        writeln!(
            code,
            "let {} = builder.add(ProducerNodeSpec::new(ProducerConfig {{ value: {:?} }}));",
            sanitize_identifier(&self.id),
            self.config.value
        )
        .map_err(|error| GraphError::Validation(format!("could not write builder code: {error}")))
    }
}

impl RegisteredNodeSpec for ScaleGraphNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &'static str {
        "scale"
    }

    fn add_to_builder(&self, builder: &mut GraphBuilder) -> BuiltNode {
        BuiltNode::Scale(builder.add(ScaleNodeSpec::new(self.config.clone())))
    }

    fn write_rust_builder_stmt(&self, code: &mut String) -> Result<(), GraphError> {
        writeln!(
            code,
            "let {} = builder.add(ScaleNodeSpec::new(ScaleConfig {{ factor: {:?} }}));",
            sanitize_identifier(&self.id),
            self.config.factor
        )
        .map_err(|error| GraphError::Validation(format!("could not write builder code: {error}")))
    }
}

pub(crate) enum BuiltNode {
    Producer(crate::framework::NodeHandle<crate::nodes::producer::ProducerNode>),
    Scale(crate::framework::NodeHandle<crate::nodes::scale::ScaleNode>),
}

impl BuiltNode {
    fn connect_to(
        &self,
        builder: &mut GraphBuilder,
        from_port: &str,
        target: &BuiltNode,
        to_port: &str,
    ) -> Result<(), GraphError> {
        match (self, from_port, target, to_port) {
            (BuiltNode::Producer(source), "value", BuiltNode::Scale(target), "value") => {
                builder.connect(source.output.value, target.input.value)
            }
            _ => Err(GraphError::Validation(format!(
                "unsupported edge {} -> {}",
                from_port, to_port
            ))),
        }
    }

    fn capture_f32_output(
        &self,
        builder: &mut GraphBuilder,
        port: &str,
    ) -> Result<mpsc::Receiver<f32>, GraphError> {
        match (self, port) {
            (BuiltNode::Scale(node), "result") => builder.capture_output(node.output.result),
            _ => Err(GraphError::Validation(format!(
                "unsupported output capture for port `{port}`"
            ))),
        }
    }
}

pub fn build_graph_from_spec(
    spec: GraphSpec,
) -> Result<(Graph, mpsc::Receiver<f32>, GraphSpec), GraphError> {
    let mut builder = GraphBuilder::new();
    let mut nodes = BTreeMap::new();

    for node in &spec.nodes {
        nodes.insert(node.id().to_owned(), node.add_to_builder(&mut builder));
    }

    for edge in &spec.edges {
        let source = nodes.get(&edge.from.node).ok_or_else(|| {
            GraphError::Validation(format!("missing source node `{}`", edge.from.node))
        })?;
        let target = nodes.get(&edge.to.node).ok_or_else(|| {
            GraphError::Validation(format!("missing target node `{}`", edge.to.node))
        })?;
        source.connect_to(&mut builder, &edge.from.port, target, &edge.to.port)?;
    }

    let scale = nodes
        .values()
        .find(|node| matches!(node, BuiltNode::Scale(_)))
        .ok_or_else(|| GraphError::Validation("graph did not contain a scale node".into()))?;

    let result = scale.capture_f32_output(&mut builder, "result")?;
    Ok((builder.build(), result, spec))
}

pub fn graph_schema() -> JsonSchema {
    schema_for::<GraphSpec>()
}

pub fn example_graph_spec() -> GraphSpec {
    GraphSpec {
        nodes: vec![
            NodeSpec::Producer(ProducerGraphNode {
                id: "producer_1".into(),
                config: ProducerConfig { value: 6.0 },
            }),
            NodeSpec::Scale(ScaleGraphNode {
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
        node.write_rust_builder_stmt(&mut code)?;
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

    if let Some(scale_id) = spec
        .nodes
        .iter()
        .find(|node| node.kind() == "scale")
        .map(|node| sanitize_identifier(node.id()))
    {
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

        builder
            .connect(producer.output.value, scale_1.input.value)
            .expect("connect scale_1");
        builder
            .connect(producer.output.value, scale_2.input.value)
            .expect("connect scale_2");
        let mut result_1 = builder
            .capture_output(scale_1.output.result)
            .expect("capture scale_1");
        let mut result_2 = builder
            .capture_output(scale_2.output.result)
            .expect("capture scale_2");

        let graph = builder.build();
        let run = tokio::spawn(graph.run());

        assert_eq!(result_1.recv().await.expect("scale_1 value"), 10.0);
        assert_eq!(result_2.recv().await.expect("scale_2 value"), 15.0);
        run.await.expect("task join").expect("graph run");
    }
}
