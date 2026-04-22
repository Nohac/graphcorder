mod graph;

extern crate self as graphcorder;

pub mod framework;
pub mod nodes;
pub mod pipeline;

pub use graphcorder_derive::{NodeInputs, NodeOutputs};
