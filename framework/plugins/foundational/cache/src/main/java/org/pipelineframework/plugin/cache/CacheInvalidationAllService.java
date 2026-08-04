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
 * Side-effect plugin that invalidates all cached entries for a given item type.
 */
public class CacheInvalidationAllService<T> implements ReactiveSideEffectService<T>, ParallelismHints {
    private static final Logger LOG = Logger.getLogger(CacheInvalidationAllService.class);

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
        String versionTag = context != null ? context.versionTag() : null;
        String prefix = resolvePrefix(item, versionTag, context);

        return cacheManager.invalidateByPrefix(prefix)
            .onItem().invoke(result -> LOG.debugf("Invalidated cache entries prefix=%s result=%s", prefix, result))
            .onFailure().invoke(failure -> LOG.error("Failed to invalidate cache prefix " + prefix, failure))
            .replaceWith(item);
    }

    private boolean shouldInvalidate(PipelineContext context) {
        if (context == null || context.replayMode() == null) {
            return false;
        }
        String value = context.replayMode().trim().toLowerCase();
        return value.equals("true") || value.equals("1") || value.equals("yes") || value.equals("replay");
    }

    private String resolvePrefix(T item, String versionTag, PipelineContext context) {
        if (cacheKeyResolver != null) {
            String resolvedKey = cacheKeyResolver.resolveKey(item, context).orElse(null);
            if (resolvedKey != null && !resolvedKey.isBlank()) {
                String className = resolvedKey;
                int separator = resolvedKey.indexOf(':');
                if (separator > 0) {
                    className = resolvedKey.substring(0, separator);
                }
                return PipelineCacheKeyFormat.applyVersionTag(className + ":", versionTag);
            }
        }
        return PipelineCacheKeyFormat.typePrefix(item.getClass(), versionTag);
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
