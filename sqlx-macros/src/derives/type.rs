use super::attributes::{
    check_strong_enum_attributes, check_struct_attributes, check_transparent_attributes,
    check_weak_enum_attributes, parse_container_attributes,
};
use quote::quote;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{
    parse_quote, Data, DataEnum, DataStruct, DeriveInput, Field, Fields, FieldsNamed,
    FieldsUnnamed, Variant,
};

pub fn expand_derive_type(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let attrs = parse_container_attributes(&input.attrs)?;
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(FieldsUnnamed { unnamed, .. }),
            ..
        }) if unnamed.len() == 1 => {
            expand_derive_has_sql_type_transparent(input, unnamed.first().unwrap())
        }
        Data::Enum(DataEnum { variants, .. }) => match attrs.repr {
            Some(_) => expand_derive_has_sql_type_weak_enum(input, variants),
            None => expand_derive_has_sql_type_strong_enum(input, variants),
        },
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => expand_derive_has_sql_type_struct(input, named),
        Data::Union(_) => Err(syn::Error::new_spanned(input, "unions are not supported")),
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(..),
            ..
        }) => Err(syn::Error::new_spanned(
            input,
            "structs with zero or more than one unnamed field are not supported",
        )),
        Data::Struct(DataStruct {
            fields: Fields::Unit,
            ..
        }) => Err(syn::Error::new_spanned(
            input,
            "unit structs are not supported",
        )),
    }
}

fn expand_derive_has_sql_type_transparent(
    input: &DeriveInput,
    field: &Field,
) -> syn::Result<proc_macro2::TokenStream> {
    check_transparent_attributes(input, field)?;

    let ident = &input.ident;
    let ty = &field.ty;

    // extract type generics
    let generics = &input.generics;
    let (_, ty_generics, _) = generics.split_for_impl();

    // add db type for clause
    let mut generics = generics.clone();
    generics.params.insert(0, parse_quote!(DB: sqlx::Database));
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#ty: sqlx::types::Type<DB>));

    let (impl_generics, _, where_clause) = generics.split_for_impl();

    Ok(quote!(
        impl #impl_generics sqlx::types::Type< DB > for #ident #ty_generics #where_clause {
            fn type_info() -> DB::TypeInfo {
                <#ty as sqlx::Type<DB>>::type_info()
            }
        }
    ))
}

fn expand_derive_has_sql_type_weak_enum(
    input: &DeriveInput,
    variants: &Punctuated<Variant, Comma>,
) -> syn::Result<proc_macro2::TokenStream> {
    let attr = check_weak_enum_attributes(input, variants)?;
    let repr = attr.repr.unwrap();
    let ident = &input.ident;

    Ok(quote!(
        impl<DB: sqlx::Database> sqlx::Type<DB> for #ident
        where
            #repr: sqlx::Type<DB>,
        {
            fn type_info() -> DB::TypeInfo {
                <#repr as sqlx::Type<DB>>::type_info()
            }
        }
    ))
}

fn expand_derive_has_sql_type_strong_enum(
    input: &DeriveInput,
    variants: &Punctuated<Variant, Comma>,
) -> syn::Result<proc_macro2::TokenStream> {
    let attributes = check_strong_enum_attributes(input, variants)?;

    let ident = &input.ident;
    let mut tts = proc_macro2::TokenStream::new();

    if cfg!(feature = "mysql") {
        tts.extend(quote!(
            impl sqlx::Type< sqlx::MySql > for #ident {
                fn type_info() -> sqlx::mysql::MySqlTypeInfo {
                    // This is really fine, MySQL is loosely typed and
                    // we don't nede to be specific here
                    <str as sqlx::Type<sqlx::MySql>>::type_info()
                }
            }
        ));
    }

    if cfg!(feature = "postgres") {
        let oid = attributes.postgres_oid.unwrap();
        tts.extend(quote!(
            impl sqlx::Type< sqlx::Postgres > for #ident {
                fn type_info() -> sqlx::postgres::PgTypeInfo {
                    sqlx::postgres::PgTypeInfo::with_oid(#oid)
                }
            }
        ));
    }

    Ok(tts)
}

fn expand_derive_has_sql_type_struct(
    input: &DeriveInput,
    fields: &Punctuated<Field, Comma>,
) -> syn::Result<proc_macro2::TokenStream> {
    let attributes = check_struct_attributes(input, fields)?;

    let ident = &input.ident;
    let mut tts = proc_macro2::TokenStream::new();

    if cfg!(feature = "postgres") {
        let oid = attributes.postgres_oid.unwrap();
        tts.extend(quote!(
            impl sqlx::types::Type< sqlx::Postgres > for #ident {
                fn type_info() -> sqlx::postgres::PgTypeInfo {
                    sqlx::postgres::PgTypeInfo::with_oid(#oid)
                }
            }
        ));
    }

    Ok(tts)
}
