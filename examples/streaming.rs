use std::time::Duration;

use facet::Facet;
use graphcorder::{
    GraphNode, NodeInputs, NodeOutputs, NodeRegistry,
    framework::{GraphError, NodeDefinition, Stream},
    static_graph,
};
/// Produces a sequence of f32 values, one at a time.
#[derive(Clone, Debug, Facet)]
struct CounterConfig {
    count: usize,
}

#[derive(NodeOutputs)]
struct CounterOutput {
    values: Stream<f32>,
}

#[derive(GraphNode)]
struct CounterNode;

impl NodeDefinition for CounterNode {
    type Config = CounterConfig;
    type Input = ();
    type Output = CounterOutput;

    async fn run(
        &self,
        _input: (),
        config: &Self::Config,
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        for i in 0..config.count {
            let value = i as f32;
            println!("[producer] sending {value}");
            output.values.send(value).await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        println!("[producer] done");
        Ok(())
    }
}

/// Receives a stream of f32 values and prints each one as it arrives.
#[derive(Clone, Debug, Facet)]
struct PrintConfig {
    label: String,
}

#[derive(NodeInputs)]
struct PrintInput {
    values: Stream<f32>,
}

#[derive(GraphNode)]
struct PrintNode;

impl NodeDefinition for PrintNode {
    type Config = PrintConfig;
    type Input = PrintInput;
    type Output = ();

    async fn run(
        &self,
        mut input: Self::Input,
        config: &Self::Config,
        _output: &mut (),
    ) -> Result<(), GraphError> {
        while let Some(v) = input.values.next().await {
            println!("[{}] received {v}", config.label);
        }
        println!("[{}] stream closed", config.label);
        Ok(())
    }
}

/// Scales each value in a stream by a factor.
#[derive(Clone, Debug, Facet)]
struct ScaleConfig {
    factor: f32,
}

#[derive(NodeInputs)]
struct ScaleInput {
    values: Stream<f32>,
}

#[derive(NodeOutputs)]
struct ScaleOutput {
    values: Stream<f32>,
}

#[derive(GraphNode)]
struct ScaleNode;

impl NodeDefinition for ScaleNode {
    type Config = ScaleConfig;
    type Input = ScaleInput;
    type Output = ScaleOutput;

    async fn run(
        &self,
        mut input: Self::Input,
        config: &Self::Config,
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        while let Some(v) = input.values.next().await {
            output.values.send(v * config.factor).await?;
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Debug, Facet, NodeRegistry)]
enum Node {
    Counter(CounterGraphNode),
    Scale(ScaleGraphNode),
    Print(PrintGraphNode),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== fan-out: one stream -> two consumers ===");
    {
        let builder = static_graph! {
            registry: Node;
            node source = CounterNode { count: 5 };
            node a = PrintNode { label: "A".into() };
            node b = PrintNode { label: "B".into() };
            connect source -> a;
            connect source -> b;
        }?;
        builder.build().run().await?;
    }

    println!("\n=== pipeline: stream -> scale -> print ===");

    {
        let builder = static_graph! {
            registry: Node;
            node source = CounterNode { count: 4 };
            node scale  = ScaleNode { factor: 10.0 };
            node sink   = PrintNode { label: "scaled".into() };
            connect source -> scale;
            connect scale  -> sink;
        }?;
        builder.build().run().await?;
    }

    Ok(())
}
