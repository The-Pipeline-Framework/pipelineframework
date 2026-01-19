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
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ParallelismHints;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.service.ReactiveSideEffectService;

/**
 * A general-purpose cache plugin that can cache any item that exposes a cache key
 * and has a corresponding CacheProvider configured in the system.
 */
public class CacheService<T> implements ReactiveSideEffectService<T>, ParallelismHints {
    private final Logger logger = Logger.getLogger(CacheService.class);

    private final CacheManager cacheManager;
    private final CacheKeyResolver cacheKeyResolver;

    @ConfigProperty(name = "pipeline.cache.policy", defaultValue = "prefer-cache")
    String policyValue;

    /**
     * Default constructor
     *
     * @param cacheManager a CacheManager object
     */
    @Inject
    public CacheService(CacheManager cacheManager, CacheKeyResolver cacheKeyResolver) {
        logger.debug("CacheService constructor called with cacheManager: " +
            (cacheManager != null ? cacheManager.getClass().getName() : "null"));
        this.cacheManager = cacheManager;
        this.cacheKeyResolver = cacheKeyResolver;
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
        String policy = overridePolicy != null ? overridePolicy : policyValue;
        CachePolicy handler = CachePolicy.fromConfig(policy, cacheManager, logger);
        if (!handler.requiresCacheKey()) {
            return handler.handle(item, null, key -> key);
        }

        logger.debugf("Using cacheManager: %s to cache item of type: %s",
            cacheManager != null ? cacheManager.getClass().getName() : "null",
            item.getClass().getName());

        assert cacheManager != null;
        String key = cacheKeyResolver.resolveKey(item, context).orElse(null);
        if (key == null || key.isBlank()) {
            PipelineCacheStatusHolder.set(CacheStatus.MISS);
            logger.warnf("No cache key strategy matched for item type %s, skipping cache", item.getClass().getName());
            return Uni.createFrom().item(item);
        }
        String versionTag = context != null ? context.versionTag() : null;
        return handler.handle(item, key, rawKey -> withVersionPrefix(rawKey, versionTag));
    }

    private String withVersionPrefix(String key, String versionTag) {
        if (versionTag == null || versionTag.isBlank()) {
            return key;
        }
        return versionTag + ":" + key;
    }

    @Override
    public OrderingRequirement orderingRequirement() {
        if (cacheManager == null) {
            return OrderingRequirement.RELAXED;
        }
        return cacheManager.orderingRequirement();
    }

    @Override
    public ThreadSafety threadSafety() {
        if (cacheManager == null) {
            return ThreadSafety.UNSAFE;
        }
        return cacheManager.threadSafety();
    }
}
