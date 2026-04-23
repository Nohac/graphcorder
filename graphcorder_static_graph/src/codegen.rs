use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, Result};

use crate::types::{ConnectDecl, Endpoint, GraphItem, NodeDecl, StaticGraphInput};

pub fn expand(input: StaticGraphInput) -> Result<TokenStream> {
    let registry = input.registry;

    let mut node_decls = Vec::new();
    let mut connect_decls = Vec::new();

    for item in input.items {
        match item {
            GraphItem::Node(node) => node_decls.push(node),
            GraphItem::Connect(connect) => connect_decls.push(connect),
        }
    }

    validate(&node_decls, &connect_decls)?;

    let node_defs = node_decls.iter().map(|node| {
        let name = &node.name;
        let expr = &node.expr;
        quote! {
            let #name = builder.add(#expr);
        }
    });

    let connect_defs = connect_decls.iter().map(|connect| {
        let source_node = &connect.source.node;
        let source_port = &connect.source.port;
        let target_node = &connect.target.node;
        let target_port = &connect.target.port;
        quote! {
            builder.connect(#source_node.output.#source_port, #target_node.input.#target_port)?;
        }
    });

    Ok(quote! {{
        let instance = ::graphcorder::init::<#registry>();
        let mut builder = instance.builder();
        #( #node_defs )*
        #( #connect_defs )*
        ::core::result::Result::<
            ::graphcorder::framework::GraphBuilder<#registry>,
            ::graphcorder::framework::GraphError,
        >::Ok(builder)
    }})
}

fn validate(node_decls: &[NodeDecl], connect_decls: &[ConnectDecl]) -> Result<()> {
    let mut errors: Option<Error> = None;
    let mut node_names = BTreeSet::new();

    for node in node_decls {
        let name = node.name.to_string();
        if !node_names.insert(name) {
            push_error(
                &mut errors,
                Error::new_spanned(&node.name, "duplicate node name"),
            );
        }
    }

    let declared_nodes = node_decls
        .iter()
        .map(|node| node.name.to_string())
        .collect::<BTreeSet<_>>();

    let mut seen_targets = BTreeMap::new();

    for connect in connect_decls {
        validate_endpoint_exists(&mut errors, &declared_nodes, &connect.source);
        validate_endpoint_exists(&mut errors, &declared_nodes, &connect.target);

        let target_key = (
            connect.target.node.to_string(),
            connect.target.port.to_string(),
        );
        if let Some(previous) = seen_targets.insert(target_key, &connect.target) {
            push_error(
                &mut errors,
                Error::new_spanned(
                    &connect.target.port,
                    format!(
                        "duplicate connection to target port `{}.{}`",
                        previous.node, previous.port
                    ),
                ),
            );
        }
    }

    if let Some(error) = errors {
        Err(error)
    } else {
        Ok(())
    }
}

fn validate_endpoint_exists(
    errors: &mut Option<Error>,
    declared_nodes: &BTreeSet<String>,
    endpoint: &Endpoint,
) {
    let node_name = endpoint.node.to_string();
    if !declared_nodes.contains(&node_name) {
        push_error(
            errors,
            Error::new_spanned(
                &endpoint.node,
                format!("unknown node `{}` in connection", endpoint.node),
            ),
        );
    }
}

fn push_error(errors: &mut Option<Error>, error: Error) {
    if let Some(existing) = errors {
        existing.combine(error);
    } else {
        *errors = Some(error);
    }
}
