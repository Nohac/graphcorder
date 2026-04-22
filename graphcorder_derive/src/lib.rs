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
            PortKind::Input => quote! { pub #name: ::graphcorder::framework::InputPort<#ty> },
            PortKind::Output => quote! { pub #name: ::graphcorder::framework::OutputPort<#ty> },
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
        quote! {
            ::graphcorder::framework::PortSchema {
                name: #lit,
                schema: schemars::schema_for!(#ty),
            }
        }
    });

    let expanded = match kind {
        PortKind::Input => {
            let receive_fields = fields.iter().map(|field| {
                let name = field.ident.as_ref().expect("named field");
                let lit = name.to_string();
                quote! { #name: runtime.receive(#lit).await? }
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
            }
        }
        PortKind::Output => {
            let send_fields = fields.iter().map(|field| {
                let name = field.ident.as_ref().expect("named field");
                let lit = name.to_string();
                quote! {
                    runtime.send(#lit, self.#name).await?;
                }
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
            }
        }
    };

    expanded.into()
}
