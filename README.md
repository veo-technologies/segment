# Veo Segment

[![Test Status](https://github.com/veo-technologies/segment/actions/workflows/rust.yml/badge.svg)](https://github.com/veo-technologies/segment/actions/workflows/rust.yml)
[![API](https://docs.rs/veo-segment/badge.svg)](https://docs.rs/veo-segment)

An unofficial, async Rust client for the [Segment](https://segment.com/docs/sources/#server)
Tracking API, maintained at [veo-technologies/segment](https://github.com/veo-technologies/segment).

## Requirements and features

`veo-segment` 0.4 targets Rust 1.88 or newer and uses the Rust 2024 edition.

The default feature set enables `rustls-tls` (reqwest 0.13's `rustls` backend)
and `gzip` (automatic gzip compression for JSON payloads >= 1024 bytes).

Optional features:
- `native-tls` / `native-tls-vendored`: use the platform TLS stack instead of `rustls-tls` (with `default-features = false`).
- `tracing`: emit `tracing` spans/events for HTTP requests and retries.

```toml
veo-segment = { version = "0.4", default-features = false, features = ["native-tls", "gzip"] }
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

The queue holds at most `max_queue_size` commands in addition to the worker's
size-limited batch and one retained overflow event. When a capacity flush fails,
the worker pauses queue consumption until a periodic retry succeeds; producers
then wait or receive `QueueFull`, according to the overflow policy.
Flush timeouts include waiting for queue space. Cancelling a close does not
cancel the shutdown task; it finishes draining or reopens the client on failure.
Call `close_and_flush` before stopping the Tokio runtime: dropping all handles
only attempts a best-effort final flush, and the queue is not durable storage.

Workspace configuration belongs to the separate `veo-segment-public-api` (`0.4`)
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

## 0.4.0 release notes

- First crates.io release under the Veo package names, with existing Rust
  library names preserved.
- Upgraded to Rust 1.88, edition 2024, reqwest 0.13, and Public API OpenAPI 74.0.1.
- Fixed non-2xx HTTP responses being reported as delivered.
- Fixed flush timeouts excluding queue admission and cancelled closes leaving
  the client stuck in its closing state.
- Preserved bounded buffering during prolonged capacity-flush failures.

## Migrating to 0.4

The Veo-maintained fork is published as `veo-segment`, not the upstream
`segment` crate. Change the dependency to `veo-segment = "0.4"`; the Rust
library name and imports remain `segment`. The management client is published as
`veo-segment-public-api`; its library name and imports remain `segment_public_api`.

Version 0.4 requires Rust 1.88 / edition 2024 and upgrades to reqwest 0.13.
Default features now include gzip. Applications using their own reqwest client
must also migrate that client to reqwest 0.13.

## License

<sup>
Licensed under <a href="LICENSE">MIT license</a>.
</sup>
