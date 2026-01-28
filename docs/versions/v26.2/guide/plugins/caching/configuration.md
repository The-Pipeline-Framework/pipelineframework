---
search: false
---

# Caching Configuration

This page covers how to enable caching and configure providers.

## Enable cache plugin

Add the cache aspect in `pipeline.yaml`:

```yaml
aspects:
  cache:
    enabled: true
    scope: GLOBAL
    position: AFTER_STEP
    order: 5
```

This enables the cache plugin for pipeline steps.

## Optional plugin host (side-effect mode)

If you want to use the cache plugin services as pipeline steps, add a plugin host:

```java
import org.pipelineframework.annotation.PipelinePlugin;

@PipelinePlugin("cache")
public class CachePluginHost {
}
```

## Application properties

Cache plugin service properties:

```
pipeline.cache.provider=caffeine
pipeline.cache.provider.class=org.pipelineframework.plugin.cache.provider.CaffeineCacheProvider
pipeline.cache.policy=prefer-cache
pipeline.cache.ttl=PT10M
pipeline.cache.caffeine.maximum-size=10000
pipeline.cache.caffeine.expire-after-write=PT30M
```

If only one provider is on the classpath, `pipeline.cache.provider` can be omitted. If a provider does
not declare hints, the framework assumes `RELAXED` ordering and `SAFE` thread safety and emits warnings.
To lock a specific
provider for production, set `pipeline.cache.provider.class` to its fully-qualified class name. For
build-time validation, pass `-Apipeline.provider.class.cache=<fqcn>` to the annotation processor.

## Cache key strategies

The cache plugin resolves keys via `CacheKeyStrategy` beans. Implement them in your application modules and assign priorities to compose multiple strategies.

```java
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;

@ApplicationScoped
public class RawDocumentKeyStrategy implements CacheKeyStrategy {
    @Override
    public Optional<String> resolveKey(Object item, PipelineContext context) {
        if (!(item instanceof RawDocument document)) {
            return Optional.empty();
        }
        if (document.rawContentHash == null || document.rawContentHash.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(document.getClass().getName() + ":" + document.rawContentHash);
    }

    @Override
    public int priority() {
        return 50;
    }
}
```

Optional: set `pipeline.cache.keyGenerator` to point at a `CacheKeyGenerator` bean. It will be consulted before the default base-key strategy, which uses `PipelineCacheKeyFormat.baseKey`.

`prefer-cache` and `return-cached` are equivalent; use either name in config or headers.
