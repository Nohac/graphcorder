use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{Error, Result, Type, spanned::Spanned};

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

    let validation_defs = validation_defs(&node_decls, &connect_decls)?;

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
        #( #validation_defs )*
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

fn validation_defs(node_decls: &[NodeDecl], connect_decls: &[ConnectDecl]) -> Result<Vec<TokenStream>> {
    let spec_types = node_decls
        .iter()
        .map(|node| (node.name.to_string(), node.spec_ty.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut defs = Vec::new();

    for connect in connect_decls {
        let source_spec = spec_types
            .get(&connect.source.node.to_string())
            .ok_or_else(|| Error::new_spanned(&connect.source.node, "unknown source node"))?;
        let target_spec = spec_types
            .get(&connect.target.node.to_string())
            .ok_or_else(|| Error::new_spanned(&connect.target.node, "unknown target node"))?;

        defs.push(validate_output_port(source_spec, &connect.source.port));
        defs.push(validate_input_port(target_spec, &connect.target.port));
    }

    for node in node_decls {
        let connected_inputs = connect_decls
            .iter()
            .filter(|connect| connect.target.node == node.name)
            .map(|connect| {
                let port = connect.target.port.to_string();
                quote! { #port }
            })
            .collect::<Vec<_>>();
        let spec_ty = &node.spec_ty;
        let span = node.name.span();
        defs.push(quote_spanned! {span=>
            const _: () = {
                type __NodeInput = <<#spec_ty as ::graphcorder::framework::GraphNodeSpec>::Node as ::graphcorder::framework::NodeDefinition>::Input;
                if ::graphcorder::framework::has_missing_required_ports(
                    <__NodeInput as ::graphcorder::framework::StaticInputPorts>::PORTS,
                    &[ #( #connected_inputs ),* ],
                ) {
                    panic!(concat!("missing required input connection on `", stringify!(#spec_ty), "`"));
                }
            };
        });
    }

    Ok(defs)
}

fn validate_output_port(spec_ty: &Type, port: &syn::Ident) -> TokenStream {
    let port_name = port.to_string();
    let span = port.span();
    quote_spanned! {span=>
        const _: () = {
            type __NodeOutput = <<#spec_ty as ::graphcorder::framework::GraphNodeSpec>::Node as ::graphcorder::framework::NodeDefinition>::Output;
            if !::graphcorder::framework::has_port(
                <__NodeOutput as ::graphcorder::framework::StaticOutputPorts>::PORTS,
                #port_name,
            ) {
                panic!(concat!("unknown output port `", #port_name, "` on `", stringify!(#spec_ty), "`"));
            }
        };
    }
}

fn validate_input_port(spec_ty: &Type, port: &syn::Ident) -> TokenStream {
    let port_name = port.to_string();
    let span = port.span();
    quote_spanned! {span=>
        const _: () = {
            type __NodeInput = <<#spec_ty as ::graphcorder::framework::GraphNodeSpec>::Node as ::graphcorder::framework::NodeDefinition>::Input;
            if !::graphcorder::framework::has_port(
                <__NodeInput as ::graphcorder::framework::StaticInputPorts>::PORTS,
                #port_name,
            ) {
                panic!(concat!("unknown input port `", #port_name, "` on `", stringify!(#spec_ty), "`"));
            }
        };
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
