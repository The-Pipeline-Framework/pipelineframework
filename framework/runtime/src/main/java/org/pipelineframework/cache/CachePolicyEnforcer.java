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

import java.util.EnumMap;
import java.util.Map;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Enforces cache policy based on cache status returned by cache plugin steps.
 */
public final class CachePolicyEnforcer {

    private static final Map<CachePolicy, CachePolicyHandler> HANDLERS =
        buildHandlers();

    private CachePolicyEnforcer() {
    }

    /**
     * Apply the configured cache policy to the current cache status.
     *
     * @param item the pipeline item to return if policy allows
     * @param <T> the item type
     * @return a Uni yielding the item or failing based on policy enforcement
     */
    public static <T> Uni<T> enforce(T item) {
        CacheStatus status = PipelineCacheStatusHolder.getAndClear();
        if (status == null) {
            return Uni.createFrom().item(item);
        }

        PipelineContext context = PipelineContextHolder.get();
        CachePolicy policy = CachePolicy.fromConfig(context != null ? context.cachePolicy() : null);
        CachePolicyHandler handler = HANDLERS.getOrDefault(policy, CachePolicyHandler.noop());
        @SuppressWarnings("unchecked")
        Uni<T> result = (Uni<T>) handler.apply(item, status);
        return result;
    }

    private static Map<CachePolicy, CachePolicyHandler> buildHandlers() {
        EnumMap<CachePolicy, CachePolicyHandler> handlers = new EnumMap<>(CachePolicy.class);
        handlers.put(CachePolicy.REQUIRE_CACHE, CachePolicyHandler.requireHit());
        handlers.put(CachePolicy.RETURN_CACHED, CachePolicyHandler.noop());
        handlers.put(CachePolicy.SKIP_IF_PRESENT, CachePolicyHandler.noop());
        handlers.put(CachePolicy.CACHE_ONLY, CachePolicyHandler.noop());
        handlers.put(CachePolicy.BYPASS_CACHE, CachePolicyHandler.noop());
        return handlers;
    }

    /**
     * Functional interface for applying cache policy decisions.
     */
    @FunctionalInterface
    public interface CachePolicyHandler {
        /**
         * Apply a cache policy decision based on the cache status.
         *
         * @param item the pipeline item under evaluation
         * @param status the cache status from the cache plugin
         * @return a Uni that yields the item or fails
         */
        Uni<Object> apply(Object item, CacheStatus status);

        /**
         * No-op handler that always returns the item.
         *
         * @return a handler that returns the item unchanged
         */
        static CachePolicyHandler noop() {
            return (item, status) -> Uni.createFrom().item(item);
        }

        /**
         * Handler that enforces a cache hit, failing otherwise.
         *
         * @return a handler that fails on cache miss or bypass
         */
        static CachePolicyHandler requireHit() {
            return (item, status) -> {
                if (status != CacheStatus.HIT) {
                    return Uni.createFrom().failure(new CachePolicyViolation(
                        "Cache policy REQUIRE_CACHE failed with status " + status));
                }
                return Uni.createFrom().item(item);
            };
        }
    }
}
