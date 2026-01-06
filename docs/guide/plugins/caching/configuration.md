# Caching Configuration

This page covers how to enable caching and configure providers.

## Enable orchestrator-side caching

Add the cache aspect in `pipeline.yaml`:

```yaml
aspects:
  cache:
    enabled: true
    scope: GLOBAL
    position: AFTER_STEP
    order: 5
```

This turns on `@CacheResult` codegen on orchestrator client steps and REST resources. It does not synthesize plugin steps.

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
pipeline.cache.policy=prefer-cache
pipeline.cache.ttl=PT10M
pipeline.cache.caffeine.maximum-size=10000
pipeline.cache.caffeine.expire-after-write=PT30M
```

If only one provider is on the classpath, `pipeline.cache.provider` can be omitted.

## Quarkus cache configuration

Orchestrator-side caching uses Quarkus Cache. Configure it with `quarkus.cache.*`:

```
quarkus.cache.caffeine."pipeline-cache".expire-after-write=PT30M
quarkus.cache.caffeine."pipeline-cache".maximum-size=10000
```

## Build-time key generator override

If you need a custom `CacheKeyGenerator`, set the annotation processor option:

```
-Apipeline.cache.keyGenerator=com.example.MyCacheKeyGenerator
```

## Per-step key generator override

Set a cache key generator on an individual step:

```java
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.cache.DocIdCacheKeyGenerator;

@PipelineStep(
    inputType = CrawlRequest.class,
    outputType = RawDocument.class,
    cacheKeyGenerator = DocIdCacheKeyGenerator.class
)
public class CrawlSourceService {
}
```

Per-step overrides take precedence over the build-time `-Apipeline.cache.keyGenerator`.

Built-in generators:

- `PipelineCacheKeyGenerator` (default)
- `DocIdCacheKeyGenerator`
- `IdCacheKeyGenerator`

These generators read the first method parameter (record accessor, getter, or field) and fall back to the default key format when the property is missing.

## Cache keys

Inputs must implement `CacheKey`:

```java
import org.pipelineframework.cache.CacheKey;

public class MyInput implements CacheKey {
    @Override
    public String cacheKey() {
        return id.toString();
    }
}
```

TPF prefixes cache keys with the input type so bulk invalidation can target a step by its input type.

`prefer-cache` and `return-cached` are equivalent; use either name in config or headers.
