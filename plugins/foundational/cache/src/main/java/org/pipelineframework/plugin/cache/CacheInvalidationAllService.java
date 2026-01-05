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
import org.pipelineframework.cache.CacheKey;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.service.ReactiveSideEffectService;

/**
 * Side-effect plugin that invalidates all cached entries for a given item type.
 */
public class CacheInvalidationAllService<T> implements ReactiveSideEffectService<T> {
    private static final Logger LOG = Logger.getLogger(CacheInvalidationAllService.class);

    @Inject
    CacheManager cacheManager;

    @Override
    public Uni<T> process(T item) {
        if (item == null) {
            return Uni.createFrom().nullItem();
        }
        if (!(item instanceof CacheKey cacheKey)) {
            LOG.warnf("Item type %s does not implement CacheKey, skipping invalidation",
                item.getClass().getName());
            return Uni.createFrom().item(item);
        }
        String key = cacheKey.cacheKey();
        if (key == null || key.isBlank()) {
            LOG.warnf("CacheKey is empty for item type %s, skipping invalidation", item.getClass().getName());
            return Uni.createFrom().item(item);
        }

        PipelineContext context = PipelineContextHolder.get();
        String versionTag = context != null ? context.versionTag() : null;
        String prefix = PipelineCacheKeyFormat.typePrefix(item.getClass(), versionTag);

        return cacheManager.invalidateByPrefix(prefix)
            .onItem().invoke(result -> LOG.debugf("Invalidated cache entries prefix=%s result=%s", prefix, result))
            .onFailure().invoke(failure -> LOG.error("Failed to invalidate cache prefix " + prefix, failure))
            .replaceWith(item);
    }
}
