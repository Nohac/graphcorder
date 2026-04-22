use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::framework::{GraphError, NodeDefinition, NodeOutputs, OutputPort, OutputRuntime, PortFactory, PortSchema};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProducerConfig {
    pub value: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProducerOutput {
    pub value: f32,
}

#[derive(Clone, Copy)]
pub struct ProducerOutputPorts {
    pub value: OutputPort<f32>,
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

impl NodeOutputs for ProducerOutput {
    type Ports = ProducerOutputPorts;

    fn ports(factory: &PortFactory) -> Self::Ports {
        ProducerOutputPorts {
            value: factory.output("value"),
        }
    }

    fn schema() -> Vec<PortSchema> {
        vec![PortSchema {
            name: "value",
            json_type: "number",
        }]
    }

    async fn send(
        self,
        runtime: &mut OutputRuntime,
    ) -> Result<(), GraphError> {
        runtime.send("value", self.value).await
    }
}
