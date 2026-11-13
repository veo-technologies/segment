# OpenAPI source

`segment-public-api-74.0.1.json` is the OpenAPI document embedded in Segment's
official [Public API reference](https://docs.segmentapis.com/) on 2026-11-13.

- Public API version: `74.0.1`
- Upstream document SHA-256:
  `e4a695614df0122aaa28e1ce02b624975c17f4c39ba0e44099ca6a92a3447b03`

The checked-in document remains byte-for-byte identical to that upstream JSON.
At build time, `build.rs` maps Segment's versioned JSON media types to the
standard `application/json` name understood by Progenitor and disambiguates
one duplicate inline schema title. It also removes presentation indentation
from descriptions so Rustdoc does not mistake prose for Rust code. These
compatibility transforms do not remove paths, operations, or component schemas.
