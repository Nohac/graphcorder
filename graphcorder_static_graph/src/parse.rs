use syn::parse::{Parse, ParseStream};
use syn::{Ident, Result, Token, Type};

use crate::types::{ConnectDecl, Endpoint, GraphItem, NodeDecl, StaticGraphInput};

mod kw {
    syn::custom_keyword!(registry);
    syn::custom_keyword!(node);
    syn::custom_keyword!(connect);
}

impl Parse for StaticGraphInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<kw::registry>()?;
        input.parse::<Token![:]>()?;
        let registry = input.parse::<Type>()?;
        input.parse::<Token![;]>()?;

        let mut items = Vec::new();
        while !input.is_empty() {
            if input.peek(kw::node) {
                items.push(GraphItem::Node(input.parse()?));
            } else if input.peek(kw::connect) {
                items.push(GraphItem::Connect(input.parse()?));
            } else {
                return Err(input.error("expected `node` or `connect`"));
            }
        }

        Ok(Self { registry, items })
    }
}

impl Parse for NodeDecl {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<kw::node>()?;
        let name = input.parse::<Ident>()?;
        input.parse::<Token![:]>()?;
        let spec_ty = input.parse()?;
        input.parse::<Token![=]>()?;
        let expr = input.parse()?;
        input.parse::<Token![;]>()?;
        Ok(Self { name, spec_ty, expr })
    }
}

impl Parse for ConnectDecl {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<kw::connect>()?;
        let source = input.parse::<Endpoint>()?;
        input.parse::<Token![->]>()?;
        let target = input.parse::<Endpoint>()?;
        input.parse::<Token![;]>()?;
        Ok(Self { source, target })
    }
}

impl Parse for Endpoint {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let node = input.parse::<Ident>()?;
        input.parse::<Token![.]>()?;
        let port = input.parse::<Ident>()?;
        Ok(Self { node, port })
    }
}
