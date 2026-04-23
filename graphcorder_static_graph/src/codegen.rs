use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, Result, Type};

use crate::types::{ConnectDecl, Endpoint, EndpointSet, GraphItem, NodeDecl, StaticGraphInput};

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
        let built_name = built_ident(name);
        let node_ty = &node.node_ty;
        let fields = &node.fields;
        quote! {
            let #name = builder.add(
                <#node_ty as ::graphcorder::framework::StaticNodeDsl>::from_config(
                    {
                        type GraphcorderConfig = <#node_ty as ::graphcorder::framework::StaticNodeDsl>::Config;
                        GraphcorderConfig { #fields }
                    }
                )
            );
            let #built_name = ::graphcorder::framework::BuiltGraphNode::new(#name.input, #name.output);
        }
    });

    let connect_defs = connect_decls
        .iter()
        .map(|connect| expand_connect(connect, &node_decls))
        .collect::<Result<Vec<_>>>()?;

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

fn expand_connect(connect: &ConnectDecl, nodes: &[NodeDecl]) -> Result<TokenStream> {
    let sources = endpoints(&connect.source);
    let targets = endpoints(&connect.target);

    match (sources.len(), targets.len()) {
        (1, 1) => expand_named_connect(&sources[0], &targets[0], nodes),
        (1, _) => {
            let source = &sources[0];
            let statements = targets
                .iter()
                .map(|target| expand_named_connect(source, target, nodes))
                .collect::<Result<Vec<_>>>()?;
            Ok(quote! { #( #statements )* })
        }
        (_, 1) => {
            let target = &targets[0];
            let statements = sources
                .iter()
                .map(|source| expand_named_connect(source, target, nodes))
                .collect::<Result<Vec<_>>>()?;
            Ok(quote! { #( #statements )* })
        }
        _ => Err(Error::new_spanned(
            targets[0].node.clone(),
            "many-to-many connect syntax is not supported",
        )),
    }
}

fn expand_named_connect(
    source: &Endpoint,
    target: &Endpoint,
    nodes: &[NodeDecl],
) -> Result<TokenStream> {
    if let (Some(source_port), Some(target_port)) = (&source.port, &target.port) {
        let source_node = &source.node;
        let target_node = &target.node;
        return Ok(quote! {
            builder.connect(
                #source_node.output.#source_port,
                #target_node.input.#target_port,
            )?;
        });
    }

    let source_built = built_ident(&source.node);
    let target_built = built_ident(&target.node);
    let source_node_ty = find_node_ty(nodes, &source.node)?;
    let target_node_ty = find_node_ty(nodes, &target.node)?;
    let source_port = source_port_expr(source, source_node_ty)?;
    let target_port = target_port_expr(target, target_node_ty)?;

    Ok(quote! {
        builder.connect_named(
            &#source_built,
            #source_port,
            &#target_built,
            #target_port,
        )?;
    })
}

fn source_port_expr(endpoint: &Endpoint, node_ty: &Type) -> Result<TokenStream> {
    if let Some(port) = &endpoint.port {
        let port_name = port.to_string();
        Ok(quote! { #port_name })
    } else {
        Ok(quote! {
            ::graphcorder::framework::only_port_name(
                <<<#node_ty as ::graphcorder::framework::StaticNodeDsl>::Node as ::graphcorder::framework::NodeDefinition>::Output as ::graphcorder::framework::StaticOutputPorts>::PORTS
            ).expect("implicit source port requires exactly one output port")
        })
    }
}

fn target_port_expr(endpoint: &Endpoint, node_ty: &Type) -> Result<TokenStream> {
    if let Some(port) = &endpoint.port {
        let port_name = port.to_string();
        Ok(quote! { #port_name })
    } else {
        Ok(quote! {
            ::graphcorder::framework::only_port_name(
                <<<#node_ty as ::graphcorder::framework::StaticNodeDsl>::Node as ::graphcorder::framework::NodeDefinition>::Input as ::graphcorder::framework::StaticInputPorts>::PORTS
            ).expect("implicit target port requires exactly one input port")
        })
    }
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

    let mut seen_targets = BTreeSet::new();

    for connect in connect_decls {
        for source in endpoints(&connect.source) {
            validate_endpoint_exists(&mut errors, &declared_nodes, source);
        }
        for target in endpoints(&connect.target) {
            validate_endpoint_exists(&mut errors, &declared_nodes, target);
            if let Some(port) = &target.port {
                let key = (target.node.to_string(), port.to_string());
                if !seen_targets.insert(key) {
                    push_error(
                        &mut errors,
                        Error::new_spanned(port, "duplicate connection to target port"),
                    );
                }
            }
        }
    }

    if let Some(error) = errors {
        Err(error)
    } else {
        Ok(())
    }
}

fn find_node_ty<'a>(nodes: &'a [NodeDecl], name: &syn::Ident) -> Result<&'a Type> {
    nodes
        .iter()
        .find(|node| node.name == *name)
        .map(|node| &node.node_ty)
        .ok_or_else(|| Error::new_spanned(name, "unknown node"))
}

fn endpoints(set: &EndpointSet) -> &[Endpoint] {
    match set {
        EndpointSet::One(endpoint) => std::slice::from_ref(endpoint),
        EndpointSet::Many(endpoints) => endpoints.as_slice(),
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

fn built_ident(name: &syn::Ident) -> syn::Ident {
    format_ident!("__graphcorder_{}_built", name)
}

fn push_error(errors: &mut Option<Error>, error: Error) {
    if let Some(existing) = errors {
        existing.combine(error);
    } else {
        *errors = Some(error);
    }
}
