# Caching

Caching in TPF is provided by cache plugins that run as side-effect steps. Enable the cache aspect to store step outputs and use invalidation aspects to control replay.

Invalidation steps only run when `x-pipeline-replay: true` is present, so normal production runs are unaffected.

## Architecture and execution flow

TPF caching has two distinct phases:

1. **Read (pre-read) phase**: the pipeline runner attempts to read from cache *before* invoking a step.
   - Only applies to **unary** steps (`StepOneToOne`).
   - If a cache hit is found, the step execution is skipped and the cached value is returned.
   - If policy is `require-cache` and no entry exists, the step fails with a cache policy violation.
2. **Write (side-effect) phase**: the cache plugin runs as a side-effect step *after* the step to store outputs.

This separation is intentional: pre-read protects expensive steps; side-effects persist outputs.

## Key resolution and target types

Cache keys are resolved via `CacheKeyStrategy` beans. When pre-reading, the runner attempts to disambiguate
strategies using the **expected output type** of the step (when available). Generated client steps for unary
operations implement `CacheKeyTarget` so the runner can prefer the correct strategy.

If no strategy declares support for the target type, TPF falls back to normal priority-based resolution.

## Replay invalidation and side-effects

Replay invalidation is a side-effect step gated by the `x-pipeline-replay` header. Because pre-read can
skip step execution, side-effect steps that must always run (such as cache invalidation) are marked
with `CacheReadBypass` to ensure they are not short-circuited by cache hits.

## Serialization and protobuf support

Redis cache entries are stored as envelopes with type metadata:

- JSON payloads are encoded/decoded using Jackson (builder-style DTOs are supported).
- Protobuf payloads are supported without reflection by registering `ProtobufMessageParser` beans.
  If no parser is registered for a protobuf type, the cache entry is ignored (treated as a miss).

## Build-time provider selection

Cache providers are registered at build time. You must set `pipeline.cache.provider` at build time (for example in `application.properties`) to include the provider bean in the application. Setting it only as a runtime env var is not enough.

## Where to go next

- [Configuration](/guide/plugins/caching/configuration)
- [Policies](/guide/plugins/caching/policies)
- [Invalidation](/guide/plugins/caching/invalidation)
- [Search replay walkthrough](/guide/plugins/caching/replay-walkthrough)
- [Cache key strategy](/guide/plugins/caching/key-strategy)
- [Cache vs persistence](/guide/plugins/caching/cache-vs-persistence)
