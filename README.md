# Segment

[![Test Status](https://github.com/veo-technologies/segment/actions/workflows/rust.yml/badge.svg)](https://github.com/veo-technologies/segment/actions/workflows/rust.yml)
[![API](https://docs.rs/segment/badge.svg)](https://docs.rs/segment)

An unofficial, async Rust client for the [Segment](https://segment.com/docs/sources/#server)
Tracking API, maintained at [veo-technologies/segment](https://github.com/veo-technologies/segment).

## Requirements and features

`segment` 0.4 targets Rust 1.88 or newer and uses the Rust 2024 edition.

The default feature set enables `rustls-tls` (reqwest 0.13's `rustls` backend)
and `gzip` (automatic gzip compression for JSON payloads >= 1024 bytes).

Optional features:
- `native-tls` / `native-tls-vendored`: use the platform TLS stack instead of `rustls-tls` (with `default-features = false`).
- `tracing`: emit `tracing` spans/events for HTTP requests and retries.

```toml
segment = { version = "0.4", default-features = false, features = ["native-tls", "gzip"] }
```

## Example usage(s)

For production services, use the bounded background client. It flushes by
message count or interval and can be drained during graceful shutdown:

```rust,no_run
use std::time::Duration;

use segment::Analytics;
use segment::message::{Track, User};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let analytics = Analytics::builder("YOUR_WRITE_KEY")
        .flush_at(20)
        .flush_interval(Duration::from_secs(5))
        .max_queue_size(10_000)
        .build()?;

    analytics.track(Track {
        user: User::user_id("user-123"),
        event: "Example Event".to_owned(),
        properties: json!({ "source": "rust" }),
        ..Default::default()
    }).await?;

    analytics.close_and_flush().await
}
```

The builder also accepts ordered `.middleware(...)` transformations for
enrichment and filtering. `analytics.subscribe()` returns a non-blocking
broadcast stream of enqueue, delivery, failure, queue-pressure, and shutdown
events for metrics and logging.

Workspace configuration belongs to the separate `segment-public-api` (`0.4`)
workspace crate. Its complete typed client is generated from Segment's official
Public API OpenAPI specification (`74.0.1`) and uses Public API bearer tokens
rather than Tracking API write keys.

For EU data residency, configure the regional endpoint via `HttpClient::builder`:

```rust,no_run
use segment::HttpClient;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let client = HttpClient::builder().eu1().build()?;
    let _ = client;
    Ok(())
}
```

The lower-level batching APIs remain available when the application owns its
delivery loop:

```rust,no_run
use segment::{HttpClient, Client, AutoBatcher, Batcher};
use segment::message::{Track, User};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let write_key = "YOUR_WRITE_KEY";

    let client = HttpClient::default();
    let batcher = Batcher::new(None);
    let mut batcher = AutoBatcher::new(client, batcher, write_key);

    // Pretend this is reading off of a queue, a file, or some other data
    // source.
    for i in 0..100 {
        let mut msg = Track::new(User::user_id(format!("user-{i}")), "Example Event");
        msg.properties = json!({
            "foo": format!("bar-{i}"),
        });

        // An error here indicates a message is too large. In real life, you
        // would probably want to put this message in a deadletter queue or some
        // equivalent.
        batcher.push(msg).await?;
    }

    batcher.flush().await?;
    Ok(())
}
```

or when you want to do struct to struct transformations

```rust,no_run
use segment::{HttpClient, Client};
use segment::message::{Track, Message, User};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let write_key = "YOUR_WRITE_KEY";

    let client = HttpClient::default();
    client.send(write_key, &Message::from(Track {
        user: User::user_id("some_user_id"),
        event: "Example Event".to_owned(),
        properties: json!({
            "some property": "some value",
            "some other property": "some other value",
        }),
        ..Default::default()
    })).await?;

    Ok(())
}
```

## License

<sup>
Licensed under <a href="LICENSE">MIT license</a>.
</sup>
