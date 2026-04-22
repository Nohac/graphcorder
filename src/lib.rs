mod graph;

extern crate self as graphcorder;

pub mod framework;

pub use graphcorder_derive::{NodeInputs, NodeOutputs};
pub use crate::graph::init;
