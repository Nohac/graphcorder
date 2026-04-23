mod graph;

extern crate self as graphcorder;

pub mod framework;

pub use graphcorder_derive::{NodeInputs, NodeOutputs};
pub use crate::graph::init;
pub use graphcorder_static_graph::static_graph;
