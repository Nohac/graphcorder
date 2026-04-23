mod codegen;
mod parse;
mod types;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro]
pub fn static_graph(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as types::StaticGraphInput);
    codegen::expand(input)
        .unwrap_or_else(|error| error.to_compile_error())
        .into()
}
