use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::framework::{GraphError, NodeDefinition};
use crate::{NodeInputs, NodeOutputs};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScaleConfig {
    pub factor: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, NodeInputs)]
pub struct ScaleInput {
    pub value: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, NodeOutputs)]
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
