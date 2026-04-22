use facet::Facet;

use crate::framework::{GraphError, NodeDefinition};
use crate::NodeOutputs;

#[derive(Clone, Debug, Facet)]
pub struct ProducerConfig {
    pub value: f32,
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
