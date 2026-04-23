# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                   # dev build
cargo build --release         # optimized build
cargo run --example basic     # run the basic example
cargo test                    # run all tests
cargo clippy                  # lint (3 known clone_on_copy warnings on PortCardinality)
cargo fmt                     # format
cargo fmt --check             # check formatting without writing
```

## Architecture

**Graphcorder** is a type-safe async dataflow graph framework. Users define typed computational nodes, wire them into graphs, and execute them concurrently. Graphs can be serialized to/from JSON and round-tripped through a declarative DSL.

### Workspace crates

| Crate | Purpose |
|---|---|
| `graphcorder` | Runtime graph engine and public API |
| `graphcorder_derive` | Proc macros: `GraphNode`, `NodeInputs`, `NodeOutputs`, `NodeRegistry` |
| `graphcorder_static_graph` | Proc macro: `static_graph!` DSL |

### Core abstractions (`src/graph.rs`)

- **`NodeDefinition` trait** — implemented by user node structs. Has associated types `Config`, `Input` (derives `NodeInputs`), `Output` (derives `NodeOutputs`), and an async `run()` method.
- **`InputPort<T>` / `OutputPort<T>`** — phantom-typed port handles. Ports support cardinalities: `Single`, `Many`, `Fixed(n)`.
- **`GraphBuilder<R>`** — programmatic graph construction; wraps a `GraphSpec<R>` and validates connections.
- **`Graph`** — the executable graph. Spawns one tokio task per node; nodes communicate via `mpsc::channel`. Uses `JoinSet` with fail-fast error propagation.
- **`GraphSpec<R>`** — JSON-serializable graph structure; uses the `facet` library for reflection-based serialization.

### Derive macro strategy (`graphcorder_derive/src/lib.rs`)

- **`#[derive(GraphNode)]`** on a unit struct: generates `{Name}GraphNode` (wraps config for registry use) and `{Name}Spec` (intermediate for the graph builder). Derives the `kind` string from the struct name by default (e.g. `ScaleNode` → `"scale"`), overridable with `#[graph_node(kind = "...")]`.
- **`#[derive(NodeInputs/NodeOutputs)]`** on named-field structs: generates `{Name}Ports` with typed port fields, schema collection, and async receive/send implementations.
- **`#[derive(NodeRegistry)]`** on an enum: each variant holds a node type; implements `RegisteredNodeSpec` for polymorphic dispatch across heterogeneous node collections.

### `static_graph!` DSL (`graphcorder_static_graph/`)

```rust
static_graph! {
    registry: MyNodeRegistry;
    node producer = ProducerNode { value: vec![1.0] };
    node scale    = ScaleNode    { factor: 2.0 };
    connect producer -> scale;
    connect scale -> [sink1, sink2];  // fan-out
}
```

`parse.rs` — custom `syn`-based parser for the DSL syntax.  
`types.rs` — AST types produced by the parser.  
`codegen.rs` — expands the AST into `GraphBuilder` call sequences; validates port names and cardinalities at macro time.

The macro returns `Result<GraphBuilder<Registry>, GraphError>`, so call sites append `?`.

### Serialization

Uses the `facet` crate (not serde) for reflection-based JSON serialization. Node configs and graph specs implement `Facet`. This enables `GraphSpec` round-trips: build in code → serialize to JSON → deserialize → execute.

### Open diagnostics work

`local-docs/next-diagnostics-todos.md` tracks planned improvements: moving `static_graph!` validation from const-error panics to proper compile-time `Diagnostic` spans, cardinality metadata, and typo suggestions via edit-distance.
