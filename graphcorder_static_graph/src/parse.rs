use syn::braced;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, FieldValue, Ident, Result, Token, Type};

use crate::types::{
    EdgeStmt, Endpoint, EndpointSet, GraphItem, NodeDecl, NodeDeclKind, StaticGraphInput,
};

mod kw {
    syn::custom_keyword!(registry);
}

impl Parse for StaticGraphInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<kw::registry>()?;
        input.parse::<Token![:]>()?;
        let registry = input.parse::<Type>()?;
        input.parse::<Token![;]>()?;

        let mut items = Vec::new();
        while !input.is_empty() {
            if input.peek(Token![let]) {
                items.push(GraphItem::Node(input.parse()?));
            } else if input.peek(Ident) || input.peek(syn::token::Bracket) {
                items.push(GraphItem::Edge(input.parse()?));
            } else {
                return Err(input.error("expected `let` or an edge statement"));
            }
        }

        Ok(Self { registry, items })
    }
}

impl Parse for NodeDecl {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<Token![let]>()?;
        let name = input.parse::<Ident>()?;
        input.parse::<Token![=]>()?;
        let kind = if is_typed_node_decl(input) {
            let node_ty = input.parse::<Type>()?;
            let content;
            braced!(content in input);
            let fields = Punctuated::<FieldValue, Token![,]>::parse_terminated(&content)?;
            NodeDeclKind::Typed { node_ty, fields }
        } else {
            NodeDeclKind::Constant(input.parse::<Expr>()?)
        };
        input.parse::<Token![;]>()?;
        Ok(Self { name, kind })
    }
}

impl Parse for EdgeStmt {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut chain = vec![input.parse::<EndpointSet>()?];
        while input.peek(Token![->]) {
            input.parse::<Token![->]>()?;
            chain.push(input.parse::<EndpointSet>()?);
        }
        input.parse::<Token![;]>()?;
        if chain.len() < 2 {
            return Err(input.error("edge statement must connect at least two endpoints"));
        }
        Ok(Self { chain })
    }
}

impl Parse for EndpointSet {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.peek(syn::token::Bracket) {
            let content;
            syn::bracketed!(content in input);
            let endpoints = Punctuated::<Endpoint, Token![,]>::parse_terminated(&content)?
                .into_iter()
                .collect();
            Ok(Self::Many(endpoints))
        } else {
            Ok(Self::One(input.parse()?))
        }
    }
}

impl Parse for Endpoint {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let node = input.parse::<Ident>()?;
        let port = if input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            Some(input.parse::<Ident>()?)
        } else {
            None
        };
        Ok(Self { node, port })
    }
}

fn is_typed_node_decl(input: ParseStream<'_>) -> bool {
    let fork = input.fork();
    fork.parse::<Type>().is_ok() && fork.peek(syn::token::Brace)
}
