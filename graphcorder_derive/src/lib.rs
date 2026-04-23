use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input};

#[proc_macro_derive(NodeRegistry)]
pub fn derive_node_registry(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = input.ident;

    let variants = match input.data {
        Data::Enum(data) => data.variants,
        _ => {
            return syn::Error::new_spanned(
                enum_name,
                "NodeRegistry derive can only be used on enums",
            )
            .to_compile_error()
            .into();
        }
    };

    let from_impls = variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let inner_ty = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().expect("field").ty
            }
            _ => {
                return syn::Error::new_spanned(
                    variant,
                    "NodeRegistry variants must be single-field tuple variants",
                )
                .to_compile_error();
            }
        };

        quote! {
            impl From<#inner_ty> for #enum_name {
                fn from(node: #inner_ty) -> Self {
                    Self::#variant_name(node)
                }
            }
        }
    });

    let id_arms = variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        quote! { Self::#variant_name(node) => ::graphcorder::framework::NodeRegistryEntry::id(node), }
    });

    let add_arms = variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        quote! { Self::#variant_name(node) => ::graphcorder::framework::NodeRegistryEntry::add_to_builder(node, builder), }
    });

    quote! {
        #( #from_impls )*

        impl ::graphcorder::framework::RegisteredNodeSpec for #enum_name {
            fn id(&self) -> &str {
                match self {
                    #( #id_arms )*
                }
            }

            fn add_to_builder(
                &self,
                builder: &mut ::graphcorder::framework::GraphBuilder<Self>,
            ) -> ::graphcorder::framework::BuiltGraphNode<Self> {
                match self {
                    #( #add_arms )*
                }
            }
        }
    }
    .into()
}

#[proc_macro_derive(GraphNode, attributes(graph_node))]
pub fn derive_graph_node(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let DeriveInput {
        ident: node_name,
        vis: node_vis,
        attrs,
        data,
        ..
    } = input;
    let spec_name = format_ident!("{}Spec", node_name);
    let node_name_string = node_name.to_string();
    let graph_node_base = node_name_string
        .strip_suffix("Node")
        .unwrap_or(&node_name_string);
    let graph_node_name = format_ident!("{}GraphNode", graph_node_base);

    let node_ctor = match data {
        Data::Struct(data) => match data.fields {
            Fields::Unit => quote! { #node_name },
            _ => {
                return syn::Error::new_spanned(
                    node_name,
                    "GraphNode derive currently requires a unit struct",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                node_name,
                "GraphNode derive can only be used on structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let kind = match attrs
        .iter()
        .find(|attr| attr.path().is_ident("graph_node"))
        .map(parse_graph_node_kind)
        .transpose()
    {
        Ok(Some(kind)) => kind,
        Ok(None) => infer_kind(&node_name.to_string()),
        Err(error) => return error.to_compile_error().into(),
    };
    let kind = LitStr::new(&kind, node_name.span());
    let config_ty = quote! { <#node_name as ::graphcorder::framework::NodeDefinition>::Config };
    let expanded = quote! {
        impl ::graphcorder::framework::NodeMeta for #node_name {
            const KIND: &'static str = #kind;
        }

        #[derive(Clone, Debug, ::facet::Facet)]
        #node_vis struct #graph_node_name {
            pub id: String,
            pub config: #config_ty,
        }

        impl #graph_node_name {
            #node_vis fn new(id: String, config: #config_ty) -> Self {
                Self { id, config }
            }
        }

        #[derive(Clone, Debug)]
        #node_vis struct #spec_name {
            config: #config_ty,
        }

        impl #spec_name {
            #node_vis fn new(config: #config_ty) -> Self {
                Self { config }
            }
        }

        impl ::graphcorder::framework::GraphNodeSpec for #spec_name {
            type Node = #node_name;
            type Registry = #graph_node_name;

            fn export_node(&self, id: String) -> Self::Registry {
                #graph_node_name::new(id, self.config.clone())
            }

            fn into_parts(self) -> (Self::Node, #config_ty) {
                (#node_ctor, self.config)
            }
        }

        impl ::graphcorder::framework::StaticNodeDsl for #node_name {
            type Config = #config_ty;
            type Node = #node_name;
            type Spec = #spec_name;

            fn from_config(config: Self::Config) -> Self::Spec {
                #spec_name::new(config)
            }
        }

        impl ::graphcorder::framework::NodeRegistryEntry for #graph_node_name {
            fn id(&self) -> &str {
                &self.id
            }

            fn add_to_builder<R>(
                &self,
                builder: &mut ::graphcorder::framework::GraphBuilder<R>,
            ) -> ::graphcorder::framework::BuiltGraphNode<R>
            where
                Self: Into<R>,
                R: ::graphcorder::framework::RegisteredNodeSpec,
            {
                ::graphcorder::framework::BuiltGraphNode::from_handle(
                    builder.add(#spec_name::new(self.config.clone())),
                )
            }
        }
    };

    expanded.into()
}

#[proc_macro_derive(NodeInputs)]
pub fn derive_node_inputs(input: TokenStream) -> TokenStream {
    derive_ports(input, PortKind::Input)
}

#[proc_macro_derive(NodeOutputs)]
pub fn derive_node_outputs(input: TokenStream) -> TokenStream {
    derive_ports(input, PortKind::Output)
}

enum PortKind {
    Input,
    Output,
}

fn parse_graph_node_kind(attr: &syn::Attribute) -> syn::Result<String> {
    let mut kind = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("kind") {
            kind = Some(meta.value()?.parse::<LitStr>()?.value());
            Ok(())
        } else {
            Err(meta.error("unsupported graph_node attribute key"))
        }
    })?;

    kind.ok_or_else(|| syn::Error::new_spanned(attr, "missing `kind = \"...\"`"))
}

fn infer_kind(name: &str) -> String {
    let base = name.strip_suffix("Node").unwrap_or(name);
    let mut result = String::new();

    for (index, character) in base.chars().enumerate() {
        if character.is_uppercase() {
            if index > 0 {
                result.push('_');
            }
            for lower in character.to_lowercase() {
                result.push(lower);
            }
        } else {
            result.push(character);
        }
    }

    result
}

fn derive_ports(input: TokenStream, kind: PortKind) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;
    let ports_name = format_ident!("{}Ports", struct_name);

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    struct_name,
                    "Node port derives require a struct with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                struct_name,
                "Node port derives can only be used on structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let port_fields = fields.iter().map(|field| {
        let name = field.ident.as_ref().expect("named field");
        let ty = &field.ty;
        match kind {
            PortKind::Input => {
                quote! { pub #name: ::graphcorder::framework::InputPort<<#ty as ::graphcorder::framework::InputPortValue>::EdgeValue> }
            }
            PortKind::Output => {
                quote! { pub #name: ::graphcorder::framework::OutputPort<<#ty as ::graphcorder::framework::OutputPortValue>::EdgeValue> }
            }
        }
    });

    let build_ports = fields.iter().map(|field| {
        let name = field.ident.as_ref().expect("named field");
        let lit = name.to_string();
        match kind {
            PortKind::Input => quote! { #name: factory.input(#lit) },
            PortKind::Output => quote! { #name: factory.output(#lit) },
        }
    });

    let schema_items = fields.iter().map(|field| {
        let name = field.ident.as_ref().expect("named field");
        let ty = &field.ty;
        let lit = name.to_string();
        match kind {
            PortKind::Input => quote! {
                <#ty as ::graphcorder::framework::InputPortValue>::schema(#lit)
            },
            PortKind::Output => quote! {
                <#ty as ::graphcorder::framework::OutputPortValue>::schema(#lit)
            },
        }
    });

    let static_port_items = fields.iter().map(|field| {
        let name = field.ident.as_ref().expect("named field");
        let lit = name.to_string();
        match kind {
            PortKind::Input => quote! {
                ::graphcorder::framework::StaticPortInfo {
                    name: #lit,
                    cardinality: ::graphcorder::framework::PortCardinality::Single,
                    required: true,
                }
            },
            PortKind::Output => quote! {
                ::graphcorder::framework::StaticPortInfo {
                    name: #lit,
                    cardinality: ::graphcorder::framework::PortCardinality::Single,
                    required: true,
                }
            },
        }
    });

    let expanded = match kind {
        PortKind::Input => {
            let receive_fields = fields.iter().map(|field| {
                let name = field.ident.as_ref().expect("named field");
                let lit = name.to_string();
                let ty = &field.ty;
                quote! { #name: <#ty as ::graphcorder::framework::InputPortValue>::receive(runtime, #lit).await? }
            });

            let input_match_arms = fields.iter().map(|field| {
                let name = field.ident.as_ref().expect("named field");
                let lit = name.to_string();
                quote! { #lit => Some(::graphcorder::framework::ErasedInputPort::new(self.#name)), }
            });

            quote! {
                #[derive(Clone, Copy)]
                pub struct #ports_name {
                    #( #port_fields, )*
                }

                impl ::graphcorder::framework::NodeInputs for #struct_name {
                    type Ports = #ports_name;

                    fn ports(factory: &::graphcorder::framework::PortFactory) -> Self::Ports {
                        #ports_name {
                            #( #build_ports, )*
                        }
                    }

                    fn schema() -> Vec<::graphcorder::framework::PortSchema> {
                        vec![ #( #schema_items, )* ]
                    }

                    async fn receive(
                        runtime: &mut ::graphcorder::framework::InputRuntime,
                    ) -> Result<Self, ::graphcorder::framework::GraphError> {
                        Ok(Self {
                            #( #receive_fields, )*
                        })
                    }
                }

                impl ::graphcorder::framework::ErasedInputPorts for #ports_name {
                    fn input_port<R: ::graphcorder::framework::RegisteredNodeSpec>(
                        &self,
                        name: &str,
                    ) -> Option<::graphcorder::framework::ErasedInputPort<R>> {
                        match name {
                            #( #input_match_arms )*
                            _ => None,
                        }
                    }
                }

                impl ::graphcorder::framework::StaticInputPorts for #struct_name {
                    const PORTS: &'static [::graphcorder::framework::StaticPortInfo] = &[
                        #( #static_port_items, )*
                    ];
                }
            }
        }
        PortKind::Output => {
            let send_fields = fields.iter().map(|field| {
                let name = field.ident.as_ref().expect("named field");
                let lit = name.to_string();
                let ty = &field.ty;
                quote! {
                    <#ty as ::graphcorder::framework::OutputPortValue>::send(self.#name, runtime, #lit).await?;
                }
            });

            let output_match_arms = fields.iter().map(|field| {
                let name = field.ident.as_ref().expect("named field");
                let lit = name.to_string();
                quote! { #lit => Some(::graphcorder::framework::ErasedOutputPort::new(self.#name)), }
            });

            quote! {
                #[derive(Clone, Copy)]
                pub struct #ports_name {
                    #( #port_fields, )*
                }

                impl ::graphcorder::framework::NodeOutputs for #struct_name {
                    type Ports = #ports_name;

                    fn ports(factory: &::graphcorder::framework::PortFactory) -> Self::Ports {
                        #ports_name {
                            #( #build_ports, )*
                        }
                    }

                    fn schema() -> Vec<::graphcorder::framework::PortSchema> {
                        vec![ #( #schema_items, )* ]
                    }

                    async fn send(
                        self,
                        runtime: &mut ::graphcorder::framework::OutputRuntime,
                    ) -> Result<(), ::graphcorder::framework::GraphError> {
                        #( #send_fields )*
                        Ok(())
                    }
                }

                impl ::graphcorder::framework::ErasedOutputPorts for #ports_name {
                    fn output_port(
                        &self,
                        name: &str,
                    ) -> Option<::graphcorder::framework::ErasedOutputPort> {
                        match name {
                            #( #output_match_arms )*
                            _ => None,
                        }
                    }
                }

                impl ::graphcorder::framework::StaticOutputPorts for #struct_name {
                    const PORTS: &'static [::graphcorder::framework::StaticPortInfo] = &[
                        #( #static_port_items, )*
                    ];
                }
            }
        }
    };

    expanded.into()
}
