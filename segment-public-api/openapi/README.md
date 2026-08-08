# OpenAPI source

`segment-public-api-73.1.0.json` is the OpenAPI document embedded in Segment's
official [Public API reference](https://docs.segmentapis.com/) on 2026-08-08.

- Public API version: `73.1.0`
- Upstream document SHA-256:
  `b7f0228e5ffd642ca0578c26b5d117d3025fa11c4018f8e062736709169cecbc`

The checked-in document remains byte-for-byte identical to that upstream JSON.
At build time, `build.rs` maps Segment's versioned JSON media types to the
standard `application/json` name understood by Progenitor and disambiguates
one duplicate inline schema title. It also removes presentation indentation
from descriptions so Rustdoc does not mistake prose for Rust code. These
compatibility transforms do not remove paths, operations, or component schemas.
