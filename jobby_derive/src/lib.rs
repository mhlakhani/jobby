//! This crate provides a procedural macro for deriving job types in jobby.
//! 
//! The `JobType` macro allows you to define enums that represent different job types, 
//! along with associated metadata such as client IDs and names. It also supports 
//! attributes for marking variants as submittable via an admin UI
//! 
//! ## Usage
//! 
//! To use this macro, annotate your enum with `#[derive(JobType)]` and provide 
//! the necessary attributes. For example:
//! 
//! ```
//! #[derive(JobType)]
//! #[job_type(u8, 1, "example", "example_module")]
//! enum ExampleJob {
//!     #[job_type(submittable)]
//!     JobA = 1,
//!     JobB = 2,
//! }
//! ```
//! 
//! This will generate implementations for converting the enum to and from 
//! its underlying representation, as well as metadata retrieval functions.

#![warn(clippy::all, clippy::pedantic, clippy::nursery)]

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    spanned::Spanned,
    Data, DeriveInput, Error, Expr, Ident, LitInt, LitStr, Meta, Result,
};

mod kw {
    syn::custom_keyword!(submittable);
}

struct JobbyVariantAttributes {
    items: syn::punctuated::Punctuated<JobbyVariantAttributeItem, syn::Token![,]>,
}

impl Parse for JobbyVariantAttributes {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        Ok(Self {
            items: input.parse_terminated(JobbyVariantAttributeItem::parse)?,
        })
    }
}

enum JobbyVariantAttributeItem {
    Submittable(VariantSubmittableAttribute),
}

impl Parse for JobbyVariantAttributeItem {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::submittable) {
            input.parse().map(Self::Submittable)
        } else {
            Err(lookahead.error())
        }
    }
}

struct VariantSubmittableAttribute {
    keyword: kw::submittable,
}

impl Parse for VariantSubmittableAttribute {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self {
            keyword: input.parse()?,
        })
    }
}

impl Spanned for VariantSubmittableAttribute {
    fn span(&self) -> Span {
        self.keyword.span()
    }
}

struct VariantInfo {
    ident: Ident,
    discriminant: Expr,
    is_submittable: bool,
    name: LitStr,
}

struct EnumInfo {
    name: Ident,
    repr: Ident,
    client_id: LitInt,
    client_name: LitStr,
    client_module: Ident,
    variants: Vec<VariantInfo>,
}

impl Parse for EnumInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok({
            let input: DeriveInput = input.parse()?;
            let name = input.ident;
            let data = match input.data {
                Data::Enum(data) => data,
                Data::Union(data) => {
                    return Err(Error::new_spanned(
                        data.union_token,
                        "Expected enum but found union",
                    ))
                }
                Data::Struct(data) => {
                    return Err(Error::new_spanned(
                        data.struct_token,
                        "Expected enum but found struct",
                    ))
                }
            };

            let mut maybe_repr: Option<Ident> = None;
            let mut maybe_client_id: Option<LitInt> = None;
            let mut maybe_client_name: Option<LitStr> = None;
            let mut maybe_client_module: Option<Ident> = None;

            for attr in input.attrs {
                let Ok(Meta::List(meta_list)) = attr.parse_meta() else {
                    continue;
                };
                let Some(ident) = meta_list.path.get_ident() else {
                    continue;
                };
                if ident == "repr" {
                    let mut nested = meta_list.nested.iter();
                    if nested.len() != 1 {
                        return Err(Error::new_spanned(
                            attr,
                            "Expected exactly one `repr` argument",
                        ));
                    }
                    let repr = nested.next().expect("We checked the length above!");
                    let repr: Ident = parse_quote! {
                        #repr
                    };
                    if repr != "u8" {
                        return Err(Error::new_spanned(repr, "JobType must be repr(u8)"));
                    }
                    maybe_repr = Some(repr);
                } else if ident == "job_type" {
                    let mut nested = meta_list.nested.iter();
                    if nested.len() != 3 {
                        return Err(Error::new_spanned(
                            attr,
                            "Expected exactly three `job_type` arguments",
                        ));
                    }
                    let client_id = nested.next().expect("We checked the length above!");
                    let client_id: LitInt = parse_quote! {
                        #client_id
                    };
                    maybe_client_id = Some(client_id);
                    let client_name = nested.next().expect("We checked the length above!");
                    let client_name: LitStr = parse_quote! {
                        #client_name
                    };
                    maybe_client_name = Some(client_name);
                    let client_module = nested.next().expect("We checked the length above!");
                    let client_module: Ident = parse_quote! {
                        #client_module
                    };
                    maybe_client_module = Some(client_module);
                } else {
                    // ignore the rest
                }
            }

            let repr = maybe_repr.ok_or_else(|| {
                Error::new(Span::call_site(), "Expected exactly one repr(u8) argument!")
            })?;
            let client_id = maybe_client_id.ok_or_else(|| {
                Error::new(
                    Span::call_site(),
                    "Expected to find valid client_id argument!",
                )
            })?;
            let client_name = maybe_client_name.ok_or_else(|| {
                Error::new(
                    Span::call_site(),
                    "Expected to find valid client_name argument!",
                )
            })?;
            let client_module = maybe_client_module.ok_or_else(|| {
                Error::new(
                    Span::call_site(),
                    "Expected to find valid client_module argument!",
                )
            })?;

            let mut variants: Vec<VariantInfo> = vec![];

            for variant in data.variants {
                let ident = variant.ident.clone();

                let discriminant = variant
                    .discriminant
                    .as_ref()
                    .map(|d| d.1.clone())
                    .ok_or_else(|| {
                        Error::new_spanned(
                            &variant,
                            "Variant must have a discriminant to ensure forward compatibility",
                        )
                    })?
                    .clone();

                let mut is_submittable = false;

                for attribute in &variant.attrs {
                    if attribute.path.is_ident("job_type") {
                        match attribute.parse_args_with(JobbyVariantAttributes::parse) {
                            Ok(variant_attributes) => {
                                for variant_attribute in variant_attributes.items {
                                    match variant_attribute {
                                        JobbyVariantAttributeItem::Submittable(_) => {
                                            is_submittable = true;
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                return Err(Error::new_spanned(
                                    attribute,
                                    format!("Invalid attribute: {err}"),
                                ));
                            }
                        }
                    }
                }

                let name = LitStr::new(&ident.to_string(), ident.span());

                variants.push(VariantInfo {
                    ident,
                    discriminant,
                    is_submittable,
                    name,
                });
            }

            Self {
                name,
                repr,
                client_id,
                client_name,
                client_module,
                variants,
            }
        })
    }
}

#[proc_macro_derive(JobType, attributes(job_type, submittable))]
pub fn derive_job_type(input: TokenStream) -> TokenStream {
    let enum_info = parse_macro_input!(input as EnumInfo);
    let name = &enum_info.name;
    let repr = &enum_info.repr;
    let client_id = &enum_info.client_id;
    let client_name = &enum_info.client_name;
    let client_module = &enum_info.client_module;
    let helper_module_name = Ident::new(
        &format!("{}_{}", "jobby_init_", &name.to_string()),
        name.span(),
    );

    let mut from_str_arms = Vec::new();
    let mut metadata_arms = Vec::new();
    for variant in enum_info.variants {
        let ident = &variant.ident;
        let output = &variant.name;
        let is_submittable = &variant.is_submittable;
        let discriminant = &variant.discriminant;
        from_str_arms.push(quote! { #name::#ident => #output });
        metadata_arms.push(quote! {
            jobby::JobTypeMetadata {
                unnamespaced_id: jobby::UnnamespacedJobType::from(Self::#ident).id(),
                base_id: #discriminant,
                client_id: #client_id,
                name: #output,
                client_name: #client_name,
                is_submittable: #is_submittable,
            }
        });
    }

    TokenStream::from(quote! {
        impl From<#name> for #repr {
            #[inline]
            fn from(enum_value: #name) -> Self {
                enum_value as Self
            }
        }

        impl From<#name> for &'static str {
            fn from(enum_value: #name) -> Self {
                match enum_value {
                    #(#from_str_arms),*
                }
            }
        }

        impl jobby::JobType for #name {
            #[inline]
            fn client_id() -> usize {
                #client_id
            }

            fn list_metadata() -> Vec<jobby::JobTypeMetadata> {
                vec![
                    #(#metadata_arms),*
                ]
            }
        }

        #[allow(non_snake_case)]
        mod #helper_module_name {
            use jobby::JobType;
            pub fn initialize(
                rocket: &jobby::rocket::Rocket<jobby::rocket::Build>,
            ) -> Result<Box<dyn jobby::Module + Send + Sync>, jobby::Error> {
                Ok(Box::new(<super::#client_module as jobby::Module>::initialize(rocket)?))
            }
            pub fn list_metadata() -> Vec<jobby::JobTypeMetadata> {
                super::#name::list_metadata()
            }
        }

        jobby::inventory::submit! {
            jobby::ClientModule::new(#client_id, #client_name, #helper_module_name::initialize, #helper_module_name::list_metadata)
        }
    })
}
