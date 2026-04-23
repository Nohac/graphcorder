mod graph;

extern crate self as graphcorder;

pub mod framework;

pub use crate::graph::init;
pub use graphcorder_derive::{GraphNode, NodeInputs, NodeOutputs};
pub use graphcorder_static_graph::static_graph;
