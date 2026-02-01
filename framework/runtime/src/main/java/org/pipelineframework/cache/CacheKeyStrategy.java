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

import java.util.Optional;

import org.pipelineframework.context.PipelineContext;

/**
 * Strategy for resolving cache keys without requiring entities to implement CacheKey.
 */
public interface CacheKeyStrategy {

    /**
     * Resolve a cache key for the given item.
     *
     * @param item the item to resolve a key for
     * @param context the current pipeline context
     * @return an optional cache key
     */
    Optional<String> resolveKey(Object item, PipelineContext context);

    /**
     * Indicates whether this strategy targets the given output type.
     *
     * <p>This is used to disambiguate strategies when pre-reading caches for a
     * specific step output type. Default is {@code false} so existing strategies
     * are only considered when no targeted strategy matches.</p>
     *
     * @param targetType expected output type for the cache entry
     * @return true if this strategy should be preferred for the target type
     */
    default boolean supportsTarget(Class<?> targetType) {
        return false;
    }

    /**
     * Strategy priority; higher values run first.
     *
     * @return priority order
     */
    default int priority() {
        return 0;
    }
}
