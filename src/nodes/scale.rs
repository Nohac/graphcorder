use facet::Facet;

use crate::framework::{GraphError, GraphNodeSpec, NodeDefinition};
use crate::{NodeInputs, NodeOutputs};

#[derive(Clone, Debug, Facet)]
pub struct ScaleConfig {
    pub factor: f32,
}

#[derive(Clone, Debug, Facet)]
pub struct ScaleNodeSpec {
    pub config: ScaleConfig,
}

#[derive(Clone, Debug, Facet, NodeInputs)]
pub struct ScaleInput {
    pub value: f32,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
pub struct ScaleOutput {
    pub result: f32,
}

pub struct ScaleNode;

impl NodeDefinition for ScaleNode {
    type Config = ScaleConfig;
    type Input = ScaleInput;
    type Output = ScaleOutput;

    const KIND: &'static str = "scale";

    async fn run(
        &self,
        input: Self::Input,
        config: &Self::Config,
    ) -> Result<Self::Output, GraphError> {
        Ok(ScaleOutput {
            result: input.value * config.factor,
        })
    }
}

impl ScaleNodeSpec {
    pub fn new(config: ScaleConfig) -> Self {
        Self { config }
    }
}

impl GraphNodeSpec for ScaleNodeSpec {
    type Node = ScaleNode;

    fn kind(&self) -> &'static str {
        ScaleNode::KIND
    }

    fn into_parts(self) -> (Self::Node, <Self::Node as NodeDefinition>::Config) {
        (ScaleNode, self.config)
    }
}
