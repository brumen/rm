extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(GeneratePrice)]
pub fn generate_price_macro(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    // Ensure the input is an enum
    let Data::Enum(data_enum) = input.data else {
        return syn::Error::new_spanned(name, "GeneratePrice can only be used with enums.")
            .to_compile_error()
            .into();
    };

    // Generate match arms for each variant
    let match_arms = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let field_binding = match &variant.fields {
            Fields::Unnamed(_) => quote! { value },
            Fields::Unit => quote! { _ },
            _ => return syn::Error::new_spanned(variant_name, "Only unnamed fields are supported.").to_compile_error().into(),
        };

        quote! {
            #name::#variant_name(#field_binding) => #field_binding.price(),
        }
    });

    // Generate the implementation
    let expanded = quote! {
        impl #name {
            pub fn price(&self) -> i32 {
                match self {
                    #( #match_arms )*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
