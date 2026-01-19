# Invalidation

Invalidation is needed for replay/rewind. TPF supports three levels:

- **Version tags**: logical invalidation by changing `x-pipeline-version`.
- **Explicit invalidation**: a dedicated invalidation step.
- **Bulk invalidation**: clear all entries for a step input type.

## Version tags (preferred)

Set a new version tag to avoid reusing stale data:

```
x-pipeline-version: v3
```

This keeps old cache entries but makes them unreachable.

## Explicit invalidation (per item)

Add an invalidation aspect that runs before the target step:

```yaml
aspects:
  cache-invalidate:
    enabled: true
    scope: STEPS
    position: BEFORE_STEP
    order: -5
    config:
      targetSteps:
        - ProcessParseDocumentService
      pluginImplementationClass: "org.pipelineframework.plugin.cache.CacheInvalidationService"
```

This uses the configured cache key strategies to invalidate a single entry.

Invalidation runs only when replay is explicitly requested:

```
x-pipeline-replay: true
```

## Bulk invalidation (by input type)

Use the bulk invalidation service to clear all entries for a step input type:

```yaml
aspects:
  cache-invalidate-all:
    enabled: true
    scope: STEPS
    position: BEFORE_STEP
    order: -5
    config:
      targetSteps:
        - ProcessParseDocumentService
      pluginImplementationClass: "org.pipelineframework.plugin.cache.CacheInvalidationAllService"
```

Bulk invalidation requires a backend that can enumerate keys (Redis or in-memory). Caffeine does not support prefix invalidation.

Bulk invalidation also requires `x-pipeline-replay: true`.

## Search example snippets

From `examples/search/config/pipeline.yaml`:

```yaml
aspects:
  cache-invalidate:
    enabled: true
    scope: "STEPS"
    position: "BEFORE_STEP"
    order: -4
    config:
      targetSteps:
        - "ProcessTokenizeContentService"
      pluginImplementationClass: "org.pipelineframework.plugin.cache.CacheInvalidationService"
  cache-invalidate-all:
    enabled: true
    scope: "STEPS"
    position: "BEFORE_STEP"
    order: -5
    config:
      targetSteps:
        - "ProcessParseDocumentService"
      pluginImplementationClass: "org.pipelineframework.plugin.cache.CacheInvalidationAllService"
```

Why these targets?

- `ProcessTokenizeContentService` invalidation is per-item: if you update tokenization logic or fix a tokenizer bug, you can rewind from the tokenize step and ensure each document is re-tokenized instead of served from cache. This clears a single document entry for that step, which is efficient during a replay pass.
- `ProcessParseDocumentService` bulk invalidation is a step-wide reset: if the HTML parser changes or you need to re-run parsing for all documents, you invalidate everything for that step input type in one go, then replay the pipeline. This avoids stale parse outputs while keeping earlier crawl data intact.

## TTL backstop

TTL is not a correctness mechanism, but a safety net:

```
pipeline.cache.ttl=PT10M
```

Use it to limit resource growth, even when relying on version tags.
