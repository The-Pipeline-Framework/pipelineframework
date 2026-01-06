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

package org.pipelineframework.plugin.cache;

import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.CacheKey;
import org.pipelineframework.cache.CacheMissException;
import org.pipelineframework.cache.CachePolicy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.service.ReactiveSideEffectService;

/**
 * A general-purpose cache plugin that can cache any item that exposes a cache key
 * and has a corresponding CacheProvider configured in the system.
 */
public class CacheService<T> implements ReactiveSideEffectService<T> {
    private final Logger logger = Logger.getLogger(CacheService.class);

    private final CacheManager cacheManager;

    @ConfigProperty(name = "pipeline.cache.policy", defaultValue = "cache-only")
    String policyValue;

    /**
     * Default constructor
     *
     * @param cacheManager a CacheManager object
     */
    @Inject
    public CacheService(CacheManager cacheManager) {
        logger.debug("CacheService constructor called with cacheManager: " +
            (cacheManager != null ? cacheManager.getClass().getName() : "null"));
        this.cacheManager = cacheManager;
    }

    @Override
    public Uni<T> process(T item) {
        logger.debugf("CacheService.process() called with item: %s (class: %s)",
            item != null ? item.toString() : "null",
            item != null ? item.getClass().getName() : "null");
        if (item == null) {
            logger.debug("Received null item to cache, returning null");
            return Uni.createFrom().nullItem();
        }

        PipelineContext context = PipelineContextHolder.get();
        String overridePolicy = context != null ? context.cachePolicy() : null;
        CachePolicy policy = CachePolicy.fromConfig(overridePolicy != null ? overridePolicy : policyValue);

        if (policy == CachePolicy.BYPASS_CACHE) {
            return Uni.createFrom().item(item);
        }

        if (!(item instanceof CacheKey cacheKey)) {
            if (policy == CachePolicy.REQUIRE_CACHE) {
                return Uni.createFrom().failure(new CacheMissException(
                    "Item type %s does not implement CacheKey".formatted(item.getClass().getName())));
            }
            logger.warnf("Item type %s does not implement CacheKey, skipping cache", item.getClass().getName());
            return Uni.createFrom().item(item);
        }

        logger.debugf("Using cacheManager: %s to cache item of type: %s",
            cacheManager != null ? cacheManager.getClass().getName() : "null",
            item.getClass().getName());

        assert cacheManager != null;
        String key = cacheKey.cacheKey();
        if (key == null || key.isBlank()) {
            if (policy == CachePolicy.REQUIRE_CACHE) {
                return Uni.createFrom().failure(new CacheMissException(
                    "CacheKey is empty for item type %s".formatted(item.getClass().getName())));
            }
            logger.warnf("CacheKey is empty for item type %s, skipping cache", item.getClass().getName());
            return Uni.createFrom().item(item);
        }
        if (context != null && context.versionTag() != null && !context.versionTag().isBlank()) {
            key = context.versionTag() + ":" + key;
        }

        String finalKey = key;
        return switch (policy) {
            case RETURN_CACHED -> cacheManager.get(key)
                .onItem().transformToUni(cached -> cached
                    .map(value -> Uni.createFrom().item((T) value))
                    .orElseGet(() -> cacheManager.cache(item).replaceWith(item)))
                .onFailure().invoke(failure -> logger.error("Failed to read from cache", failure))
                .onItem().transform(result -> result != null ? result : item);
            case SKIP_IF_PRESENT -> cacheManager.exists(key)
                .onItem().transformToUni(exists -> exists
                    ? Uni.createFrom().item(item)
                    : cacheManager.cache(item).replaceWith(item))
                .onFailure().invoke(failure -> logger.error("Failed to cache item", failure));
            case REQUIRE_CACHE -> cacheManager.get(key)
                .onItem().transformToUni(cached -> cached
                    .map(value -> Uni.createFrom().item((T) value))
                    .orElseGet(() -> Uni.createFrom().failure(new CacheMissException(
                        "Cache entry missing for key %s".formatted(finalKey)))))
                .onFailure().invoke(failure -> logger.error("Failed to read from cache", failure));
            case BYPASS_CACHE -> Uni.createFrom().item(item);
            case CACHE_ONLY -> cacheManager.cache(item)
                .onItem().invoke(result -> logger.debugf("Cached item of type: %s", result != null ? result.getClass().getName() : "null"))
                .onFailure().invoke(failure -> logger.error("Failed to cache item", failure))
                .replaceWith(item);
        };
    }

}
