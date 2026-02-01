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

    public Optional<String> resolveKey(Object item, PipelineContext context) {
        return resolveKey(item, context, null);
    }

    public Optional<String> resolveKey(Object item, PipelineContext context, Class<?> targetType) {
        if (item == null) {
            return Optional.empty();
        }
        List<CacheKeyStrategy> ordered = strategies.stream()
            .sorted(Comparator.comparingInt(CacheKeyStrategy::priority).reversed())
            .toList();
        if (targetType != null) {
            boolean hasSupportingStrategy = false;
            for (CacheKeyStrategy strategy : ordered) {
                if (!strategy.supportsTarget(targetType)) {
                    continue;
                }
                hasSupportingStrategy = true;
                Optional<String> resolved = strategy.resolveKey(item, context);
                if (resolved.isPresent()) {
                    String key = resolved.get();
                    if (!key.isBlank()) {
                        return Optional.of(key.trim());
                    }
                }
            }
            if (hasSupportingStrategy) {
                return Optional.empty();
            }
        }
        // Graceful degradation: if no strategy explicitly supports the target type, try all strategies
        for (CacheKeyStrategy strategy : ordered) {
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
