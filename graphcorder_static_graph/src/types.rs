use syn::{Expr, Ident, Type};

pub struct StaticGraphInput {
    pub registry: Type,
    pub items: Vec<GraphItem>,
}

pub enum GraphItem {
    Node(NodeDecl),
    Connect(ConnectDecl),
}

pub struct NodeDecl {
    pub name: Ident,
    pub expr: Expr,
}

pub struct ConnectDecl {
    pub source: Endpoint,
    pub target: Endpoint,
}

pub struct Endpoint {
    pub node: Ident,
    pub port: Ident,
}
