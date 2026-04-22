use facet::Facet;

use crate::framework::{GraphError, GraphNodeSpec, NodeDefinition};
use crate::NodeOutputs;

#[derive(Clone, Debug, Facet)]
pub struct ProducerConfig {
    pub value: f32,
}

#[derive(Clone, Debug, Facet)]
pub struct ProducerNodeSpec {
    pub id: String,
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
        Self {
            id: String::new(),
            config,
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }
}

impl GraphNodeSpec for ProducerNodeSpec {
    type Node = ProducerNode;

    fn into_parts(self) -> (Self::Node, <Self::Node as NodeDefinition>::Config) {
        (ProducerNode, self.config)
    }
}
