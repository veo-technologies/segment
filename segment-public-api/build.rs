use std::{env, fs, path::PathBuf};

use progenitor::{GenerationSettings, Generator, InterfaceStyle, TagStyle};
use serde_json::Value;

const SPEC_PATH: &str = "openapi/segment-public-api-74.0.1.json";

fn main() {
    println!("cargo:rerun-if-changed={SPEC_PATH}");

    let source = fs::read_to_string(SPEC_PATH).expect("could not read Segment OpenAPI document");
    let mut value: Value =
        serde_json::from_str(&source).expect("Segment OpenAPI document is not valid JSON");
    normalize_vendor_json_content_types(&mut value);
    disambiguate_inline_schema_titles(&mut value);
    sanitize_doc_descriptions(&mut value);
    let spec: openapiv3::OpenAPI =
        serde_json::from_value(value).expect("Segment OpenAPI document is not OpenAPI 3");

    let mut settings = GenerationSettings::default();
    settings
        .with_interface(InterfaceStyle::Positional)
        .with_tag(TagStyle::Merged);
    let mut generator = Generator::new(&settings);
    let tokens = generator
        .generate_tokens(&spec)
        .expect("could not generate the Segment Public API client");
    let syntax = syn::parse2(tokens).expect("generated Segment Public API client is invalid Rust");
    let generated = prettyplease::unparse(&syntax);

    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo did not set OUT_DIR"))
        .join("segment_public_api.rs");
    fs::write(output, generated).expect("could not write generated Segment Public API client");
}

// `PersonalizationInput.profile`, `PersonalizationOutput.profile`, and the
// top-level `Profile` component all normalize to the Rust name `Profile`.
// Preserve distinct shapes under descriptive generated names.
fn disambiguate_inline_schema_titles(value: &mut Value) {
    for (pointer, replacement) in [
        (
            "/components/schemas/PersonalizationInput/properties/profile/title",
            "PersonalizationInputProfile",
        ),
        (
            "/components/schemas/PersonalizationOutput/properties/profile/title",
            "PersonalizationOutputProfile",
        ),
    ] {
        if let Some(title) = value.pointer_mut(pointer) {
            *title = Value::String(replacement.to_owned());
        }
    }
}

// Redoc descriptions contain presentation indentation that rustdoc interprets
// as Rust code blocks. Remove that indentation outside explicit fences and
// mark unlabelled fences as text so generated documentation remains testable.
fn sanitize_doc_descriptions(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                sanitize_doc_descriptions(value);
            }
        }
        Value::Object(object) => {
            if let Some(Value::String(description)) = object.get_mut("description") {
                let mut in_fence = false;
                *description = description
                    .lines()
                    .map(|line| {
                        let trimmed = line.trim_start();
                        if trimmed.starts_with("```") {
                            let output = if !in_fence && trimmed == "```" {
                                "```text"
                            } else {
                                trimmed
                            };
                            in_fence = !in_fence;
                            output.to_owned()
                        } else if in_fence {
                            line.to_owned()
                        } else {
                            trimmed.to_owned()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
            }
            for value in object.values_mut() {
                sanitize_doc_descriptions(value);
            }
        }
        _ => {}
    }
}

// Segment publishes the same JSON schema under standard and versioned vendor
// media types. Progenitor models one representation per response, so select
// Segment's stable representation and expose it as application/json. Endpoints
// that only have beta or alpha representations retain that newest available
// schema.
fn normalize_vendor_json_content_types(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_vendor_json_content_types(value);
            }
        }
        Value::Object(object) => {
            if let Some(Value::Object(content)) = object.get_mut("content")
                && !content.is_empty()
                && content.keys().all(|key| key.starts_with("application/"))
            {
                let selected = [
                    "application/vnd.segment.v1+json",
                    "application/json",
                    "application/vnd.segment.v1beta+json",
                    "application/vnd.segment.v1alpha+json",
                ]
                .into_iter()
                .find_map(|key| content.get(key).cloned());
                if let Some(selected) = selected {
                    content.clear();
                    content.insert("application/json".to_owned(), selected);
                }
            }
            for value in object.values_mut() {
                normalize_vendor_json_content_types(value);
            }
        }
        _ => {}
    }
}
