use facet::Facet;

use crate::framework::{GraphError, GraphNodeSpec, NodeDefinition};
use crate::NodeInputs;

#[derive(Clone, Debug, Facet)]
pub struct PrintConfig {
    pub label: String,
}

#[derive(Clone, Debug, Facet)]
pub struct PrintNodeSpec {
    pub config: PrintConfig,
}

#[derive(Clone, Debug, Facet, NodeInputs)]
pub struct PrintInput {
    pub value: f32,
}

pub struct PrintNode;

impl NodeDefinition for PrintNode {
    type Config = PrintConfig;
    type Input = PrintInput;
    type Output = ();

    const KIND: &'static str = "print";

    async fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
    ) -> Result<Self::Output, GraphError> {
        println!("{}: {}", config.label, input.value);
        Ok(())
    }
}

impl PrintNodeSpec {
    pub fn new(config: PrintConfig) -> Self {
        Self { config }
    }
}

impl GraphNodeSpec for PrintNodeSpec {
    type Node = PrintNode;

    fn kind(&self) -> &'static str {
        PrintNode::KIND
    }

    fn export_node(&self, id: String) -> crate::pipeline::NodeSpec {
        crate::pipeline::NodeSpec::Print(crate::pipeline::PrintGraphNode {
            id,
            config: self.config.clone(),
        })
    }

    fn into_parts(self) -> (Self::Node, <Self::Node as NodeDefinition>::Config) {
        (PrintNode, self.config)
    }
}
