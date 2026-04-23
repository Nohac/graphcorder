use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

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
