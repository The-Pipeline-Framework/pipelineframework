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
import org.pipelineframework.cache.CacheMissException;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;

final class RequireCachePolicy implements CachePolicy {
    private final CacheManager cacheManager;
    private final Logger logger;

    RequireCachePolicy(CacheManager cacheManager, Logger logger) {
        this.cacheManager = cacheManager;
        this.logger = logger;
    }

    @Override
    public <T> Uni<T> handle(T item, String rawKey, UnaryOperator<String> keyResolver) {
        String key = keyResolver.apply(rawKey);
        return cacheManager.get(key)
            .onItem().transformToUni(cached -> cached
                .map(value -> withStatus(CacheStatus.HIT, Uni.createFrom().item((T) value)))
                .orElseGet(() -> {
                    PipelineCacheStatusHolder.set(CacheStatus.MISS);
                    return Uni.createFrom().failure(
                        new CacheMissException("Required cache entry missing for key: " + key));
                }))
            .onFailure().invoke(failure -> logger.error("Failed to read from cache", failure));
    }

    private <T> Uni<T> withStatus(CacheStatus status, Uni<T> uni) {
        return uni.onItem().invoke(() -> PipelineCacheStatusHolder.set(status));
    }
}
