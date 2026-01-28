---
search: false
---

# Caching

Caching in TPF is provided by cache plugins that run as side-effect steps. Enable the cache aspect to store step outputs and use invalidation aspects to control replay.

Invalidation steps only run when `x-pipeline-replay: true` is present, so normal production runs are unaffected.

## Where to go next

- [Configuration](/versions/v26.2/guide/plugins/caching/configuration)
- [Policies](/versions/v26.2/guide/plugins/caching/policies)
- [Invalidation](/versions/v26.2/guide/plugins/caching/invalidation)
- [Search replay walkthrough](/versions/v26.2/guide/plugins/caching/replay-walkthrough)
- [Cache key strategy](/versions/v26.2/guide/plugins/caching/key-strategy)
- [Cache vs persistence](/versions/v26.2/guide/plugins/caching/cache-vs-persistence)
