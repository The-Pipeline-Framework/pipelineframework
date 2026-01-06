/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.cache;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.arc.Unremovable;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheKeyGenerator;
import io.quarkus.cache.CacheManager;
import io.quarkus.cache.CaffeineCache;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Applies cache policies using the Quarkus cache for generated pipeline steps.
 */
@ApplicationScoped
@Unremovable
public class PipelineCacheSupport {

    private static final Logger LOG = Logger.getLogger(PipelineCacheSupport.class);

    @Inject
    CacheManager cacheManager;

    @ConfigProperty(name = "pipeline.cache.caffeine.name", defaultValue = "pipeline-cache")
    String cacheName;

    @ConfigProperty(name = "pipeline.cache.policy", defaultValue = "cache-only")
    String defaultPolicy;

    public <T> Uni<T> apply(
        Class<? extends CacheKeyGenerator> generatorClass,
        Object[] params,
        Supplier<Uni<T>> compute) {

        if (compute == null) {
            return Uni.createFrom().nullItem();
        }

        CachePolicy policy = resolvePolicy();
        if (policy == CachePolicy.BYPASS_CACHE) {
            return compute.get();
        }

        String key = resolveKey(generatorClass, params);
        if (key == null || key.isBlank()) {
            if (policy == CachePolicy.REQUIRE_CACHE) {
                return Uni.createFrom().failure(new CacheMissException("Cache key is empty."));
            }
            return compute.get();
        }

        CaffeineCache cache = resolveCache();
        if (cache == null) {
            return compute.get();
        }

        return switch (policy) {
            case RETURN_CACHED -> readOrCompute(cache, key, compute);
            case REQUIRE_CACHE -> requireCached(cache, key);
            case SKIP_IF_PRESENT -> computeSkipIfPresent(cache, key, compute);
            case CACHE_ONLY -> computeAndCache(cache, key, compute);
            case BYPASS_CACHE -> compute.get();
        };
    }

    private CachePolicy resolvePolicy() {
        PipelineContext context = PipelineContextHolder.get();
        String overridePolicy = context != null ? context.cachePolicy() : null;
        return CachePolicy.fromConfig(overridePolicy != null ? overridePolicy : defaultPolicy);
    }

    private String resolveKey(Class<? extends CacheKeyGenerator> generatorClass, Object[] params) {
        CacheKeyGenerator generator = resolveGenerator(generatorClass);
        if (generator == null) {
            return null;
        }
        Object key = generator.generate(null, params);
        return key == null ? null : String.valueOf(key);
    }

    private CacheKeyGenerator resolveGenerator(Class<? extends CacheKeyGenerator> generatorClass) {
        Class<? extends CacheKeyGenerator> resolved =
            generatorClass == null || generatorClass == CacheKeyGenerator.class
                ? PipelineCacheKeyGenerator.class
                : generatorClass;

        InstanceHandle<? extends CacheKeyGenerator> handle = Arc.container().instance(resolved);
        if (handle.isAvailable()) {
            return handle.get();
        }

        try {
            return resolved.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            LOG.warnf("Failed to instantiate cache key generator %s: %s",
                resolved.getName(), e.getMessage());
            return null;
        }
    }

    private CaffeineCache resolveCache() {
        if (cacheManager == null) {
            return null;
        }
        Cache cache = cacheManager.getCache(cacheName).orElse(null);
        if (cache == null) {
            return null;
        }
        return cache.as(CaffeineCache.class);
    }

    private <T> Uni<T> readOrCompute(CaffeineCache cache, String key, Supplier<Uni<T>> compute) {
        return Uni.createFrom().completionStage(cache.getIfPresent(key))
            .onItem().transformToUni(cached -> {
                if (cached != null) {
                    @SuppressWarnings("unchecked")
                    T value = (T) cached;
                    return Uni.createFrom().item(value);
                }
                return computeAndCache(cache, key, compute);
            });
    }

    private <T> Uni<T> requireCached(CaffeineCache cache, String key) {
        return Uni.createFrom().completionStage(cache.getIfPresent(key))
            .onItem().transformToUni(cached -> {
                if (cached == null) {
                    return Uni.createFrom().failure(new CacheMissException(
                        "Cache entry missing for key " + key));
                }
                @SuppressWarnings("unchecked")
                T value = (T) cached;
                return Uni.createFrom().item(value);
            });
    }

    private <T> Uni<T> computeSkipIfPresent(CaffeineCache cache, String key, Supplier<Uni<T>> compute) {
        return Uni.createFrom().completionStage(cache.getIfPresent(key))
            .onItem().transform(cached -> cached != null)
            .onItem().transformToUni(exists -> compute.get()
                .onItem().invoke(result -> {
                    if (!exists) {
                        cacheValue(cache, key, result);
                    }
                }));
    }

    private <T> Uni<T> computeAndCache(CaffeineCache cache, String key, Supplier<Uni<T>> compute) {
        return compute.get()
            .onItem().invoke(result -> cacheValue(cache, key, result));
    }

    private void cacheValue(CaffeineCache cache, String key, Object value) {
        if (cache == null || key == null || key.isBlank() || value == null) {
            return;
        }
        cache.put(key, CompletableFuture.completedFuture(value));
    }
}
