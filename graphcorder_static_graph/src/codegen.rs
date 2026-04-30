use std::collections::BTreeSet;

use proc_macro2::{Span, TokenStream, TokenTree};
use quote::{format_ident, quote};
use syn::{Error, Result};

use crate::types::{
    EdgeStmt, Endpoint, EndpointSet, GraphItem, NodeDecl, NodeDeclKind, StaticGraphInput,
};

pub fn expand(input: StaticGraphInput) -> Result<TokenStream> {
    let registry = input.registry;

    let mut node_decls = Vec::new();
    let mut connect_decls = Vec::new();

    for item in input.items {
        match item {
            GraphItem::Node(node) => node_decls.push(node),
            GraphItem::Edge(edge) => connect_decls.push(edge),
        }
    }

    validate(&node_decls, &connect_decls)?;

    let validations = expand_validation_module(&node_decls, &connect_decls)?;

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

    let mut next_edge_id = 0;
    let connect_defs = connect_decls
        .iter()
        .map(|connect| expand_edge_stmt(connect, &node_decls, &mut next_edge_id))
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {{
        let instance = ::graphcorder::init::<#registry>();
        let mut builder = instance.builder();
        #validations
        #( #node_defs )*
        #( #connect_defs )*
        ::core::result::Result::<
            ::graphcorder::framework::GraphBuilder<#registry>,
            ::graphcorder::framework::GraphError,
        >::Ok(builder)
    }})
}

fn expand_edge_stmt(
    edge: &EdgeStmt,
    nodes: &[NodeDecl],
    next_edge_id: &mut usize,
) -> Result<TokenStream> {
    let mut statements = Vec::new();

    for pair in edge.chain.windows(2) {
        statements.push(expand_connect_pair(
            &pair[0],
            &pair[1],
            nodes,
            next_edge_id,
        )?);
    }

    Ok(quote! { #( #statements )* })
}

fn expand_connect_pair(
    source_set: &EndpointSet,
    target_set: &EndpointSet,
    nodes: &[NodeDecl],
    next_edge_id: &mut usize,
) -> Result<TokenStream> {
    let sources = endpoints(source_set);
    let targets = endpoints(target_set);

    match (sources.len(), targets.len()) {
        (1, 1) => expand_named_connect(&sources[0], &targets[0], nodes, next_edge_id),
        (1, _) => {
            let source = &sources[0];
            let statements = targets
                .iter()
                .map(|target| expand_named_connect(source, target, nodes, next_edge_id))
                .collect::<Result<Vec<_>>>()?;
            Ok(quote! { #( #statements )* })
        }
        (_, 1) => {
            let target = &targets[0];
            let statements = sources
                .iter()
                .map(|source| expand_named_connect(source, target, nodes, next_edge_id))
                .collect::<Result<Vec<_>>>()?;
            Ok(quote! { #( #statements )* })
        }
        _ => Err(Error::new_spanned(
            targets[0].node.clone(),
            "many-to-many edge syntax is not supported",
        )),
    }
}

fn expand_named_connect(
    source: &Endpoint,
    target: &Endpoint,
    nodes: &[NodeDecl],
    next_edge_id: &mut usize,
) -> Result<TokenStream> {
    let source_expr = source_port_expr(source);
    let target_expr = target_port_expr(target);
    let edge_id = *next_edge_id;
    *next_edge_id += 1;
    let source_port = format_ident!("__graphcorder_edge_{edge_id}_source");
    let target_port = format_ident!("__graphcorder_edge_{edge_id}_target");
    let source_node = find_node_decl(nodes, &source.node)?;

    match source_node.kind {
        NodeDeclKind::Constant(_) => Ok(quote! {
            let #source_port = #source_expr;
            let #target_port = #target_expr;
            builder.connect_constant_source(#source_port, #target_port)?;
        }),
        NodeDeclKind::Typed { .. } => Ok(quote! {
            let #source_port = #source_expr;
            let #target_port = #target_expr;
            builder.connect(#source_port, #target_port)?;
        }),
    }
}

fn expand_validation_module(nodes: &[NodeDecl], edges: &[EdgeStmt]) -> Result<TokenStream> {
    let endpoint_validations = expand_endpoint_validations(nodes, edges)?;
    let input_validations = nodes
        .iter()
        .map(|node| expand_required_input_validation(node, edges, nodes))
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        #[doc(hidden)]
        #[allow(non_snake_case, non_upper_case_globals)]
        mod __graphcorder_static_graph_validation {
            use super::*;
            #( #endpoint_validations )*
            #( #input_validations )*
        }
    })
}

fn expand_endpoint_validations(
    nodes: &[NodeDecl],
    edges: &[EdgeStmt],
) -> Result<Vec<TokenStream>> {
    let mut seen = BTreeSet::new();
    let mut validations = Vec::new();

    for edge in edges {
        for pair in edge.chain.windows(2) {
            for source in endpoints(&pair[0]) {
                push_endpoint_validation(&mut seen, &mut validations, source, nodes, true)?;
            }
            for target in endpoints(&pair[1]) {
                push_endpoint_validation(&mut seen, &mut validations, target, nodes, false)?;
            }
        }
    }

    Ok(validations)
}

fn push_endpoint_validation(
    seen: &mut BTreeSet<(String, bool, Option<String>)>,
    validations: &mut Vec<TokenStream>,
    endpoint: &Endpoint,
    nodes: &[NodeDecl],
    is_source: bool,
) -> Result<()> {
    let key = (
        endpoint.node.to_string(),
        is_source,
        endpoint.port.as_ref().map(ToString::to_string),
    );
    if seen.insert(key) {
        validations.push(expand_endpoint_validation(endpoint, nodes, is_source)?);
    }
    Ok(())
}

fn expand_endpoint_validation(
    endpoint: &Endpoint,
    nodes: &[NodeDecl],
    is_source: bool,
) -> Result<TokenStream> {
    let node = find_node_decl(nodes, &endpoint.node)?;
    let ports_expr = ports_expr(node, is_source);

    if let Some(port) = &endpoint.port {
        let port_name = port.to_string();
        Ok(quote! {
            const _: () = {
                ::graphcorder::framework::validate_static_port_exists(#ports_expr, #port_name);
            };
        })
    } else {
        Ok(quote! {
            const _: () = {
                ::graphcorder::framework::validate_static_implicit_port(#ports_expr, #is_source);
            };
        })
    }
}

fn expand_required_input_validation(
    node: &NodeDecl,
    connect_decls: &[EdgeStmt],
    nodes: &[NodeDecl],
) -> Result<TokenStream> {
    let connected_ports = connect_decls
        .iter()
        .flat_map(edge_targets)
        .filter(|target| target.node == node.name)
        .map(|target| resolved_target_port_expr(&target, nodes))
        .collect::<Result<Vec<_>>>()?;
    let ports_expr = ports_expr(node, false);

    Ok(quote! {
        const _: () = {
            ::graphcorder::framework::validate_static_input_connections(
                #ports_expr,
                &[ #( #connected_ports ),* ],
            );
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

fn validate(node_decls: &[NodeDecl], connect_decls: &[EdgeStmt]) -> Result<()> {
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
        for pair in connect.chain.windows(2) {
            for source in endpoints(&pair[0]) {
                validate_endpoint_exists(&mut errors, &declared_nodes, source);
            }
            for target in endpoints(&pair[1]) {
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
    let tokens = match &node.kind {
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
    };

    neutralize_spans(tokens)
}

fn neutralize_spans(tokens: TokenStream) -> TokenStream {
    tokens
        .into_iter()
        .map(|token| match token {
            TokenTree::Group(group) => {
                let mut group =
                    proc_macro2::Group::new(group.delimiter(), neutralize_spans(group.stream()));
                group.set_span(Span::call_site());
                TokenTree::Group(group)
            }
            TokenTree::Ident(mut ident) => {
                ident.set_span(Span::call_site());
                TokenTree::Ident(ident)
            }
            TokenTree::Punct(mut punct) => {
                punct.set_span(Span::call_site());
                TokenTree::Punct(punct)
            }
            TokenTree::Literal(mut literal) => {
                literal.set_span(Span::call_site());
                TokenTree::Literal(literal)
            }
        })
        .collect()
}

fn endpoints(set: &EndpointSet) -> &[Endpoint] {
    match set {
        EndpointSet::One(endpoint) => std::slice::from_ref(endpoint),
        EndpointSet::Many(endpoints) => endpoints.as_slice(),
    }
}

fn edge_targets(edge: &EdgeStmt) -> impl Iterator<Item = Endpoint> + '_ {
    edge.chain
        .windows(2)
        .flat_map(|pair| endpoints(&pair[1]).iter().cloned())
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
