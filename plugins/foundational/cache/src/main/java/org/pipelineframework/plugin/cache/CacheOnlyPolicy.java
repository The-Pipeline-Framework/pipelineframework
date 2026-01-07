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

import java.util.function.UnaryOperator;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;

final class CacheOnlyPolicy implements CachePolicy {
    private final CacheManager cacheManager;
    private final Logger logger;

    CacheOnlyPolicy(CacheManager cacheManager, Logger logger) {
        this.cacheManager = cacheManager;
        this.logger = logger;
    }

    @Override
    public <T> Uni<T> handle(T item, String rawKey, UnaryOperator<String> keyResolver) {
        return cacheManager.cache(item)
            .onItem().invoke(result -> {
                PipelineCacheStatusHolder.set(CacheStatus.WRITE);
                logger.debugf("Cached item of type: %s", result != null ? result.getClass().getName() : "null");
            })
            .onFailure().invoke(failure -> logger.error("Failed to cache item", failure))
            .replaceWith(item);
    }
}
