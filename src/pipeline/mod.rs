use std::collections::BTreeMap;

use enum_dispatch::enum_dispatch;
use facet::Facet;

use crate::framework::{Graph, GraphBuilder, GraphError};
pub use crate::nodes::print::PrintConfig;
pub use crate::nodes::print::PrintNodeSpec;
pub use crate::nodes::producer::ProducerConfig;
pub use crate::nodes::producer::ProducerNodeSpec;
pub use crate::nodes::scale::ScaleConfig;
pub use crate::nodes::scale::ScaleNodeSpec;

#[derive(Clone, Debug, Facet)]
pub struct GraphSpec {
    pub nodes: Vec<NodeSpec>,
    pub edges: Vec<EdgeSpec>,
}

#[enum_dispatch]
pub(crate) trait RegisteredNodeSpec {
    fn id(&self) -> &str;
    fn add_to_builder(&self, builder: &mut GraphBuilder) -> BuiltNode;
}

#[repr(C)]
#[derive(Clone, Debug, Facet)]
#[enum_dispatch(RegisteredNodeSpec)]
pub enum NodeSpec {
    Print(PrintGraphNode),
    Producer(ProducerGraphNode),
    Scale(ScaleGraphNode),
}

impl NodeSpec {
    pub fn id(&self) -> &str {
        RegisteredNodeSpec::id(self)
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
pub struct PrintGraphNode {
    pub id: String,
    pub config: PrintConfig,
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

    fn add_to_builder(&self, builder: &mut GraphBuilder) -> BuiltNode {
        BuiltNode::Producer(builder.add(ProducerNodeSpec::new(self.config.clone())))
    }
}

impl RegisteredNodeSpec for PrintGraphNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder(&self, builder: &mut GraphBuilder) -> BuiltNode {
        BuiltNode::Print(builder.add(PrintNodeSpec::new(self.config.clone())))
    }
}

impl RegisteredNodeSpec for ScaleGraphNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn add_to_builder(&self, builder: &mut GraphBuilder) -> BuiltNode {
        BuiltNode::Scale(builder.add(ScaleNodeSpec::new(self.config.clone())))
    }
}

pub(crate) enum BuiltNode {
    Print(crate::framework::NodeHandle<crate::nodes::print::PrintNode>),
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
            (BuiltNode::Scale(source), "result", BuiltNode::Print(target), "value") => {
                builder.connect(source.output.result, target.input.value)
            }
            _ => Err(GraphError::Validation(format!(
                "unsupported edge {} -> {}",
                from_port, to_port
            ))),
        }
    }
}

pub fn build_graph_from_spec(
    spec: GraphSpec,
) -> Result<Graph, GraphError> {
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

    Ok(builder.build())
}
