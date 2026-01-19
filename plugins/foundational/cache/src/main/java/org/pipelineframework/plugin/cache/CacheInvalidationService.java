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
import org.jboss.logging.Logger;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ParallelismHints;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.service.ReactiveSideEffectService;

/**
 * Side-effect plugin that invalidates cached entries using configured cache key strategies.
 */
public class CacheInvalidationService<T> implements ReactiveSideEffectService<T>, ParallelismHints {
    private static final Logger LOG = Logger.getLogger(CacheInvalidationService.class);

    @Inject
    CacheManager cacheManager;
    @Inject
    CacheKeyResolver cacheKeyResolver;

    @Override
    public Uni<T> process(T item) {
        if (item == null) {
            return Uni.createFrom().nullItem();
        }
        PipelineContext context = PipelineContextHolder.get();
        if (!shouldInvalidate(context)) {
            return Uni.createFrom().item(item);
        }
        String baseKey = cacheKeyResolver.resolveKey(item, context).orElse(null);
        if (baseKey == null || baseKey.isBlank()) {
            LOG.warnf("No cache key strategy matched for item type %s, skipping invalidation",
                item.getClass().getName());
            return Uni.createFrom().item(item);
        }
        String versionTag = context != null ? context.versionTag() : null;
        String key = PipelineCacheKeyFormat.applyVersionTag(baseKey, versionTag);

        String key1 = key;
        String key2 = key;
        return cacheManager.invalidate(key)
            .onItem().invoke(result -> LOG.debugf("Invalidated cache entry=%s result=%s", key1, result))
            .onFailure().invoke(failure -> LOG.error("Failed to invalidate cache entry " + key2, failure))
            .replaceWith(item);
    }

    private boolean shouldInvalidate(PipelineContext context) {
        if (context == null || context.replayMode() == null) {
            return false;
        }
        String value = context.replayMode().trim().toLowerCase();
        return value.equals("true") || value.equals("1") || value.equals("yes") || value.equals("replay");
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
