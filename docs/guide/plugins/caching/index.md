# Caching

Caching in TPF is provided by cache plugins that run as side-effect steps. Enable the cache aspect to store step outputs and use invalidation aspects to control replay.

Invalidation steps only run when `x-pipeline-replay: true` is present, so normal production runs are unaffected.

## Where to go next

- [Configuration](/guide/plugins/caching/configuration)
- [Policies](/guide/plugins/caching/policies)
- [Invalidation](/guide/plugins/caching/invalidation)
- [Search replay walkthrough](/guide/plugins/caching/replay-walkthrough)
- [Cache key strategy](/guide/plugins/caching/key-strategy)
- [Cache vs persistence](/guide/plugins/caching/cache-vs-persistence)
