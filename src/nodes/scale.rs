use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::framework::{
    GraphError, InputPort, InputRuntime, NodeDefinition, NodeInputs, NodeOutputs, OutputPort,
    OutputRuntime, PortFactory, PortSchema,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScaleConfig {
    pub factor: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScaleInput {
    pub value: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScaleOutput {
    pub result: f32,
}

#[derive(Clone, Copy)]
pub struct ScaleInputPorts {
    pub value: InputPort<f32>,
}

#[derive(Clone, Copy)]
pub struct ScaleOutputPorts {
    pub result: OutputPort<f32>,
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

impl NodeInputs for ScaleInput {
    type Ports = ScaleInputPorts;

    fn ports(factory: &PortFactory) -> Self::Ports {
        ScaleInputPorts {
            value: factory.input("value"),
        }
    }

    fn schema() -> Vec<PortSchema> {
        vec![PortSchema {
            name: "value",
            json_type: "number",
        }]
    }

    async fn receive(
        runtime: &mut InputRuntime,
    ) -> Result<Self, GraphError> {
        Ok(Self {
            value: runtime.receive("value").await?,
        })
    }
}

impl NodeOutputs for ScaleOutput {
    type Ports = ScaleOutputPorts;

    fn ports(factory: &PortFactory) -> Self::Ports {
        ScaleOutputPorts {
            result: factory.output("result"),
        }
    }

    fn schema() -> Vec<PortSchema> {
        vec![PortSchema {
            name: "result",
            json_type: "number",
        }]
    }

    async fn send(
        self,
        runtime: &mut OutputRuntime,
    ) -> Result<(), GraphError> {
        runtime.send("result", self.result).await
    }
}
