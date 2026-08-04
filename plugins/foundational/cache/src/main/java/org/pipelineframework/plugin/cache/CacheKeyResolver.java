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

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;

@ApplicationScoped
public class CacheKeyResolver {

    @Inject
    Instance<CacheKeyStrategy> strategies;

    /**
     * Resolves a cache key for the given item using the registered CacheKeyStrategy instances.
     *
     * @param item    the object for which to resolve a cache key
     * @param context the pipeline context provided to strategies during resolution
     * @return an Optional containing the resolved, trimmed cache key if one is found and non-blank, otherwise an empty Optional
     */
    public Optional<String> resolveKey(Object item, PipelineContext context) {
        return resolveKey(item, context, null);
    }

    /**
     * Resolve a cache key for the given item and pipeline context, optionally preferring strategies that support a specific target type.
     *
     * @param item the object to derive a cache key from
     * @param context the pipeline context passed to strategies during resolution
     * @param targetType if non-null, only strategies that declare support for this target type are attempted first; if at least one such strategy exists but none produce a non-blank key, an empty result is returned; if no strategy declares support, resolution falls back to trying all strategies
     * @return an Optional containing the trimmed cache key when a non-blank key is produced, or Optional.empty() otherwise
     */
    public Optional<String> resolveKey(Object item, PipelineContext context, Class<?> targetType) {
        if (item == null) {
            return Optional.empty();
        }
        List<CacheKeyStrategy> ordered = strategies.stream()
            .sorted(Comparator.comparingInt(CacheKeyStrategy::priority).reversed())
            .toList();
        if (targetType != null) {
            List<CacheKeyStrategy> supporting = ordered.stream()
                .filter(strategy -> strategy.supportsTarget(targetType))
                .toList();
            if (!supporting.isEmpty()) {
                Optional<String> resolved = resolveFromStrategies(supporting, item, context);
                if (resolved.isPresent()) {
                    return resolved;
                }
                return Optional.empty();
            }
        }
        // Graceful degradation: if no strategy explicitly supports the target type, try all strategies
        return resolveFromStrategies(ordered, item, context);
    }

    private Optional<String> resolveFromStrategies(
        List<CacheKeyStrategy> strategies,
        Object item,
        PipelineContext context) {
        for (CacheKeyStrategy strategy : strategies) {
            Optional<String> resolved = strategy.resolveKey(item, context);
            if (resolved.isPresent()) {
                String key = resolved.get();
                if (!key.isBlank()) {
                    return Optional.of(key.trim());
                }
            }
        }
        return Optional.empty();
    }
}
