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

/**
 * Cache policy handler for cache plugin execution.
 */
@FunctionalInterface
public interface CachePolicy {

    <T> Uni<T> handle(T item, String rawKey, UnaryOperator<String> keyResolver);

    default boolean requiresCacheKey() {
        return true;
    }

    static CachePolicy fromConfig(String policy, CacheManager cacheManager, Logger logger) {
        org.pipelineframework.cache.CachePolicy resolved =
            org.pipelineframework.cache.CachePolicy.fromConfig(policy);
        return switch (resolved) {
            case RETURN_CACHED -> new ReturnCachedPolicy(cacheManager, logger);
            case SKIP_IF_PRESENT -> new SkipIfPresentPolicy(cacheManager, logger);
            case REQUIRE_CACHE -> new RequireCachePolicy(cacheManager, logger);
            case CACHE_ONLY -> new CacheOnlyPolicy(cacheManager, logger);
            case BYPASS_CACHE -> new BypassCachePolicy();
        };
    }
}
