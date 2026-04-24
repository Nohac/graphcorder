use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{Error, Result};

use crate::types::{
    ConnectDecl, Endpoint, EndpointSet, GraphItem, NodeDecl, NodeDeclKind, StaticGraphInput,
};

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

    let port_validations = connect_decls
        .iter()
        .map(|connect| expand_port_validation(connect, &node_decls))
        .collect::<Result<Vec<_>>>()?;
    let node_validations = node_decls
        .iter()
        .map(|node| expand_required_input_validation(node, &connect_decls, &node_decls))
        .collect::<Result<Vec<_>>>()?;

    let node_defs = node_decls.iter().map(|node| {
        let name = &node.name;
        match &node.kind {
            NodeDeclKind::Typed { node_ty, fields } => quote! {
                let #name = builder.add(
                    <#node_ty as ::graphcorder::framework::StaticNodeDsl>::from_config(
                        {
                            type GraphcorderConfig = <#node_ty as ::graphcorder::framework::StaticNodeDsl>::Config;
                            GraphcorderConfig { #fields }
                        }
                    )
                );
            },
            NodeDeclKind::Constant(expr) => quote! {
                let #name = builder.add(::graphcorder::framework::constant(#expr));
            },
        }
    });

    let connect_defs = connect_decls
        .iter()
        .map(|connect| expand_connect(connect, &node_decls))
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {{
        let instance = ::graphcorder::init::<#registry>();
        let mut builder = instance.builder();
        #( #port_validations )*
        #( #node_validations )*
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
    _nodes: &[NodeDecl],
) -> Result<TokenStream> {
    let source_expr = source_port_expr(source);
    let target_expr = target_port_expr(target);
    let span = endpoint_span(target);

    Ok(quote_spanned! {span=>
        builder.connect(#source_expr, #target_expr)?;
    })
}

fn expand_port_validation(connect: &ConnectDecl, nodes: &[NodeDecl]) -> Result<TokenStream> {
    let source_validations = endpoints(&connect.source)
        .iter()
        .map(|source| expand_endpoint_validation(source, nodes, true))
        .collect::<Result<Vec<_>>>()?;
    let target_validations = endpoints(&connect.target)
        .iter()
        .map(|target| expand_endpoint_validation(target, nodes, false))
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        #( #source_validations )*
        #( #target_validations )*
    })
}

fn expand_endpoint_validation(
    endpoint: &Endpoint,
    nodes: &[NodeDecl],
    is_source: bool,
) -> Result<TokenStream> {
    let node = find_node_decl(nodes, &endpoint.node)?;
    let ports_expr = ports_expr(node, is_source);
    let role = if is_source { "output" } else { "input" };

    if let Some(port) = &endpoint.port {
        let port_name = port.to_string();
        let message = format!("unknown {role} port `{port_name}`");
        let span = port.span();
        Ok(quote_spanned! {span=>
            const _: () = {
                if !::graphcorder::framework::has_port(#ports_expr, #port_name) {
                    panic!(#message);
                }
            };
        })
    } else {
        let message = if is_source {
            "implicit source port requires exactly one output port"
        } else {
            "implicit target port requires exactly one input port"
        };
        let span = endpoint.node.span();
        Ok(quote_spanned! {span=>
            const _: () = {
                if ::graphcorder::framework::only_port_name(#ports_expr).is_none() {
                    panic!(#message);
                }
            };
        })
    }
}

fn expand_required_input_validation(
    node: &NodeDecl,
    connect_decls: &[ConnectDecl],
    nodes: &[NodeDecl],
) -> Result<TokenStream> {
    let connected_ports = connect_decls
        .iter()
        .flat_map(|connect| endpoints(&connect.target).iter().cloned())
        .filter(|target| target.node == node.name)
        .map(|target| resolved_target_port_expr(&target, nodes))
        .collect::<Result<Vec<_>>>()?;
    let ports_expr = ports_expr(node, false);

    let span = node.name.span();
    Ok(quote_spanned! {span=>
        const _: () = {
            const PORTS: &[::graphcorder::framework::StaticPortInfo] = #ports_expr;
            const CONNECTED: &[&str] = &[ #( #connected_ports ),* ];
            if ::graphcorder::framework::has_duplicate_single_connections(PORTS, CONNECTED) {
                panic!("duplicate connection to single input port");
            }
            if ::graphcorder::framework::has_missing_required_ports(PORTS, CONNECTED) {
                panic!("node is missing required input connections");
            }
        };
    })
}

fn source_port_expr(endpoint: &Endpoint) -> TokenStream {
    let node = &endpoint.node;
    if let Some(port) = &endpoint.port {
        quote! { #node.output.#port }
    } else {
        quote! { #node.single_output_port() }
    }
}

fn target_port_expr(endpoint: &Endpoint) -> TokenStream {
    let node = &endpoint.node;
    if let Some(port) = &endpoint.port {
        quote! { #node.input.#port }
    } else {
        quote! { #node.single_input_port() }
    }
}

fn resolved_target_port_expr(endpoint: &Endpoint, nodes: &[NodeDecl]) -> Result<TokenStream> {
    let node = find_node_decl(nodes, &endpoint.node)?;
    if let Some(port) = &endpoint.port {
        let port_name = port.to_string();
        Ok(quote! { #port_name })
    } else {
        let ports_expr = ports_expr(node, false);
        Ok(quote! {
            ::graphcorder::framework::only_port_name(#ports_expr)
                .expect("implicit target port requires exactly one input port")
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

fn find_node_decl<'a>(nodes: &'a [NodeDecl], name: &syn::Ident) -> Result<&'a NodeDecl> {
    nodes
        .iter()
        .find(|node| node.name == *name)
        .ok_or_else(|| Error::new_spanned(name, "unknown node"))
}

fn ports_expr(node: &NodeDecl, is_source: bool) -> TokenStream {
    match &node.kind {
        NodeDeclKind::Typed { node_ty, .. } => {
            if is_source {
                quote! {
                    <<<#node_ty as ::graphcorder::framework::StaticNodeDsl>::Node as ::graphcorder::framework::NodeDefinition>::Output as ::graphcorder::framework::StaticOutputPorts>::PORTS
                }
            } else {
                quote! {
                    <<<#node_ty as ::graphcorder::framework::StaticNodeDsl>::Node as ::graphcorder::framework::NodeDefinition>::Input as ::graphcorder::framework::StaticInputPorts>::PORTS
                }
            }
        }
        NodeDeclKind::Constant(_) => {
            if is_source {
                quote! { <::graphcorder::framework::ConstantValue as ::graphcorder::framework::StaticOutputPorts>::PORTS }
            } else {
                quote! { <() as ::graphcorder::framework::StaticInputPorts>::PORTS }
            }
        }
    }
}

fn endpoints(set: &EndpointSet) -> &[Endpoint] {
    match set {
        EndpointSet::One(endpoint) => std::slice::from_ref(endpoint),
        EndpointSet::Many(endpoints) => endpoints.as_slice(),
    }
}

fn endpoint_span(endpoint: &Endpoint) -> proc_macro2::Span {
    endpoint
        .port
        .as_ref()
        .map(|port| port.span())
        .unwrap_or_else(|| endpoint.node.span())
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
