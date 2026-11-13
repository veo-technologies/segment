# Segment Public API client

`segment-public-api` is a generated, typed Rust client for Segment's
management-plane Public API. It is intentionally separate from the `segment`
crate, which sends events to the Tracking API with a source write key.

The complete client surface is generated from Segment Public API OpenAPI
`74.0.1`, including Sources, Destinations, Warehouses, Tracking Plans, Catalog,
IAM, Audiences, Functions, and usage APIs.

```rust,no_run
use segment_public_api::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::with_token("YOUR_PUBLIC_API_TOKEN")?;

    // Every operation and request/response model in Segment's OpenAPI document
    // is available through `client` and `segment_public_api::types`.
    let response = client.list_sources(None).await?;
    println!("{:#?}", response.into_inner());
    Ok(())
}
```

The token is installed as a sensitive `Authorization: Bearer ...` default
header and is redacted from builder debug output. Use
`Client::authenticated_builder` to select another endpoint or adjust timeouts.
