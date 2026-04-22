use facet::Facet;

use crate::framework::{GraphError, GraphNodeSpec, NodeDefinition};
use crate::NodeOutputs;

#[derive(Clone, Debug, Facet)]
pub struct ProducerConfig {
    pub value: f32,
}

#[derive(Clone, Debug, Facet)]
pub struct ProducerNodeSpec {
    pub config: ProducerConfig,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
pub struct ProducerOutput {
    pub value: f32,
}

pub struct ProducerNode;

impl NodeDefinition for ProducerNode {
    type Config = ProducerConfig;
    type Input = ();
    type Output = ProducerOutput;

    const KIND: &'static str = "producer";

    async fn run(
        &self,
        _input: Self::Input,
        config: &Self::Config,
    ) -> Result<Self::Output, GraphError> {
        Ok(ProducerOutput { value: config.value })
    }
}

impl ProducerNodeSpec {
    pub fn new(config: ProducerConfig) -> Self {
        Self { config }
    }
}

impl GraphNodeSpec for ProducerNodeSpec {
    type Node = ProducerNode;

    fn kind(&self) -> &'static str {
        ProducerNode::KIND
    }

    fn export_node(&self, id: String) -> crate::pipeline::NodeSpec {
        crate::pipeline::NodeSpec::Producer(crate::pipeline::ProducerGraphNode {
            id,
            config: self.config.clone(),
        })
    }

    fn into_parts(self) -> (Self::Node, <Self::Node as NodeDefinition>::Config) {
        (ProducerNode, self.config)
    }
}
