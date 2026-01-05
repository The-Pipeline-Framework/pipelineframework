# Caching

Caching in TPF is a hybrid feature: it can be enabled at build time for the orchestrator, or used as an explicit plugin step. Most projects will use the orchestrator mode because it avoids remote calls and keeps caching out of service code.

## Two modes, one feature

**Orchestrator-side caching (default)**
- Enabled by the `cache` aspect.
- No synthetic steps are generated.
- Client steps (and REST resources) are annotated with `@CacheResult`.
- Best for replay/rewind and latency reduction because cache hits skip the remote call.

**Plugin side-effect caching (explicit)**
- Use `CacheService` or invalidation services as dedicated steps.
- Synthesizes pipeline steps like any other plugin.
- Useful when you want cache writes or invalidation to be visible as pipeline stages.

You can combine both: orchestrator-side caching for fast hits, plus explicit invalidation steps when you rewind.

## Where to go next

- [Configuration](/guide/plugins/caching/configuration)
- [Policies](/guide/plugins/caching/policies)
- [Invalidation](/guide/plugins/caching/invalidation)
- [Search replay walkthrough](/guide/plugins/caching/replay-walkthrough)
- [Cache key strategy](/guide/plugins/caching/key-strategy)
- [Cache vs persistence](/guide/plugins/caching/cache-vs-persistence)
