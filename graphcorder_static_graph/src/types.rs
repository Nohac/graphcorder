use syn::{Expr, FieldValue, Ident, Token, Type, punctuated::Punctuated};

pub struct StaticGraphInput {
    pub registry: Type,
    pub items: Vec<GraphItem>,
}

pub enum GraphItem {
    Node(NodeDecl),
    Edge(EdgeStmt),
}

pub struct NodeDecl {
    pub name: Ident,
    pub kind: NodeDeclKind,
}

pub enum NodeDeclKind {
    Typed {
        node_ty: Type,
        fields: Punctuated<FieldValue, Token![,]>,
    },
    Constant(Expr),
}

pub struct EdgeStmt {
    pub chain: Vec<EndpointSet>,
}

pub enum EndpointSet {
    One(Endpoint),
    Many(Vec<Endpoint>),
}

#[derive(Clone)]
pub struct Endpoint {
    pub node: Ident,
    pub port: Option<Ident>,
}
