# Cache Policies

Caching policies control how the orchestrator treats cache hits and writes.

## Policies

Set `pipeline.cache.policy`:

- `cache-only`: always cache the item and continue
- `return-cached`: return cached value if present, otherwise compute and cache
- `skip-if-present`: if the key exists, skip caching and return the original item

## Policy decision flow

```mermaid
flowchart TD
  A[Input arrives] --> B{Policy}
  B -->|cache-only| C[Run step]
  C --> D[Store output in cache]
  D --> E[Continue]

  B -->|return-cached| F{Cache hit?}
  F -->|Yes| G[Return cached output]
  F -->|No| C

  B -->|skip-if-present| H{Cache hit?}
  H -->|Yes| I[Return input unchanged]
  H -->|No| C
```

## Per-request overrides

You can override policy using headers:

```
x-pipeline-cache-policy: return-cached
```

Headers are propagated by the orchestrator to downstream steps.

## Version tags

Use `x-pipeline-version` to segregate cache keys during replay:

```
x-pipeline-version: v2
```

This provides logical invalidation without purging old entries.
