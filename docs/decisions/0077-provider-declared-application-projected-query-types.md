---
title: Providers declare application-projected Query types
status: accepted
---

# ADR-0077: Providers declare application-projected Query types

## Context

ADR-0028 requires exact provider type contracts when an application binds a Block capability. The LLM Query connector is a deliberate exception: it accepts application-authored input and produces an application-authored result, validating and projecting the result against the pinned canonical type at the Query boundary. A compiler check for the `llm.query` provider name and `decide` operation obscured this contract and coupled the compiler to one provider.

Block-aware Java and protobuf generation also needs imported Block declarations before parsing the effective template. A second template loader wrapped the canonical `PipelineTemplateConfigLoader`, creating another entry point for the same parse.

## Decision

An operation's published type contract has an explicit mode in provider manifest schema 8. `EXACT` remains the default for existing manifests and operations. A Query provider may declare `APPLICATION_PROJECTED` only when its Java signature publishes `Object` input and output. This mode permits application-owned canonical types at a Block binding; the provider remains responsible for validating the input and materializing the declared output. The compiler reads the mode from pinned provider metadata, never from a provider name or operation name. Native Query capture and replay remain unchanged. The mode participates in callable snapshot identity, while existing exact-contract fingerprints remain stable.

`PipelineTemplateConfigLoader` remains the sole template-loading entry point. It accepts an optional prepared source with deterministic cleanup. The compiler owns Block import and supplies that prepared source during Java and protobuf generation. The contracts module does not discover Block packages or depend on the compiler.

## Consequences

- Exact type matching remains the default and is required for ordinary providers and Commands.
- Application projection is an explicit provider promise, visible in generated metadata and release contracts. Invalid `Object` signatures fail during metadata construction or reading.
- Build-time checks validate the selected provider, operation, configuration and authored canonical type linkage. The provider validates projected values at execution time; arbitrary model output is never accepted merely because the metadata declares this mode.
- Generator source preparation does not create another parser or runtime Block path. Prepared temporary sources are closed after the canonical load.

This refines ADR-0028's exact-type requirement for explicitly declared application-projected Query operations; its application authority and native Query/Command semantics remain in force.
