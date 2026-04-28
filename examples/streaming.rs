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

/// Produces values into a stream with an explicit channel buffer capacity of 2.
/// The `Stream<f32, 2>` means at most 2 values can be buffered before the producer
/// blocks waiting for a consumer to catch up.
#[derive(NodeOutputs)]
struct BoundedCounterOutput {
    values: Stream<f32, 2>,
}

#[derive(GraphNode)]
struct BoundedCounterNode;

impl NodeDefinition for BoundedCounterNode {
    type Config = CounterConfig;
    type Input = ();
    type Output = BoundedCounterOutput;

    async fn run(
        &self,
        _input: (),
        config: &Self::Config,
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        for i in 0..config.count {
            let value = i as f32;
            println!("[bounded producer] sending {value}");
            output.values.send(value).await?;
        }
        println!("[bounded producer] done");
        Ok(())
    }
}

/// Consumer that matches the bounded producer's Stream<f32, 2> port type.
#[derive(NodeInputs)]
struct BoundedPrintInput {
    values: Stream<f32, 2>,
}

#[derive(GraphNode)]
struct BoundedPrintNode;

impl NodeDefinition for BoundedPrintNode {
    type Config = PrintConfig;
    type Input = BoundedPrintInput;
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
        Ok(())
    }
}

/// Emits a single scalar f32 value — used to demonstrate scalar → Stream<f32> connection.
#[derive(Clone, Debug, Facet)]
struct SingleValueConfig {
    value: f32,
}

#[derive(Clone, Debug, Facet, NodeOutputs)]
struct SingleValueOutput {
    values: f32,
}

#[derive(GraphNode)]
struct SingleValueNode;

impl NodeDefinition for SingleValueNode {
    type Config = SingleValueConfig;
    type Input = ();
    type Output = SingleValueOutput;

    async fn run(
        &self,
        _input: (),
        config: &Self::Config,
        output: &mut Self::Output,
    ) -> Result<(), GraphError> {
        output.values = config.value;
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Debug, Facet, NodeRegistry)]
enum Node {
    Counter(CounterGraphNode),
    BoundedCounter(BoundedCounterGraphNode),
    Scale(ScaleGraphNode),
    Print(PrintGraphNode),
    BoundedPrint(BoundedPrintGraphNode),
    SingleValue(SingleValueGraphNode),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== fan-out: one stream -> two consumers ===");
    {
        let builder = static_graph! {
            registry: Node;
            let source = CounterNode { count: 5 };
            let scale  = ScaleNode { factor: 10.0 };
            let a = PrintNode { label: "A".into() };
            let b = PrintNode { label: "B".into() };
            source -> [scale, b];
            scale -> a;
        }?;
        builder.build().run().await?;
    }

    println!("\n=== pipeline: stream -> scale -> print ===");
    {
        let builder = static_graph! {
            registry: Node;
            let source = CounterNode { count: 4 };
            let scale  = ScaleNode { factor: 10.0 };
            let sink   = PrintNode { label: "scaled".into() };
            source -> scale -> sink;
        }?;
        builder.build().run().await?;
    }

    println!("\n=== bounded stream (capacity 2) ===");
    {
        let builder = static_graph! {
            registry: Node;
            let source = BoundedCounterNode { count: 6 };
            let sink   = BoundedPrintNode { label: "bounded".into() };
            source -> sink;
        }?;
        builder.build().run().await?;
    }

    // Stream<f32> output -> Stream<f32, 4> input: different N values are compatible.
    println!("\n=== cross-N: Stream<f32> -> Stream<f32, 4> ===");
    {
        let builder = static_graph! {
            registry: Node;
            let source = CounterNode { count: 3 };
            let sink   = BoundedPrintNode { label: "cross-N".into() };
            source -> sink;
        }?;
        builder.build().run().await?;
    }

    // f32 scalar output -> Stream<f32> input: consumer sees a one-element stream.
    println!("\n=== scalar f32 -> Stream<f32> ===");
    {
        let builder = static_graph! {
            registry: Node;
            let source = SingleValueNode { value: 42.0 };
            let sink   = PrintNode { label: "from-scalar".into() };
            source -> sink;
        }?;
        builder.build().run().await?;
    }

    Ok(())
}
