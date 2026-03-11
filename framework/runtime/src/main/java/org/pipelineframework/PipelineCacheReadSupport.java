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

package org.pipelineframework;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.cache.CachePolicy;
import org.pipelineframework.cache.PipelineCacheReader;
import org.pipelineframework.context.PipelineContext;

class PipelineCacheReadSupport {

    private final PipelineCacheReader reader;
    private final List<CacheKeyStrategy> strategies;
    private final String defaultPolicy;

    PipelineCacheReadSupport(PipelineCacheReader reader, List<CacheKeyStrategy> strategies, String defaultPolicy) {
        this.reader = Objects.requireNonNull(reader, "reader must not be null");
        this.strategies = List.copyOf(Objects.requireNonNull(strategies, "strategies must not be null"));
        this.defaultPolicy = Objects.requireNonNull(defaultPolicy, "defaultPolicy must not be null");
    }

    Optional<String> resolveKey(Object item, PipelineContext context) {
        return resolveKey(item, context, null);
    }

    Optional<String> resolveKey(Object item, PipelineContext context, Class<?> targetType) {
        if (item == null) {
            return Optional.empty();
        }
        // Key resolution is target-aware first: if any strategy supports the requested target type,
        // only those strategies participate and an all-empty result means an explicit no-match.
        // If no strategy supports the target type, fall back to the full strategy list.
        boolean foundSupporting = false;
        if (targetType != null) {
            for (CacheKeyStrategy strategy : strategies) {
                if (!strategy.supportsTarget(targetType)) {
                    continue;
                }
                foundSupporting = true;
                Optional<String> resolved = strategy.resolveKey(item, context);
                if (resolved.isPresent()) {
                    String key = resolved.get();
                    if (!key.isBlank()) {
                        return Optional.of(key.trim());
                    }
                }
            }
            if (foundSupporting) {
                return Optional.empty();
            }
        }
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

    CachePolicy resolvePolicy(PipelineContext context) {
        String policy = context != null ? context.cachePolicy() : defaultPolicy;
        return CachePolicy.fromConfig(policy);
    }

    boolean shouldRead(CachePolicy policy) {
        if (policy == null) {
            return false;
        }
        return policy == CachePolicy.RETURN_CACHED
            || policy == CachePolicy.REQUIRE_CACHE
            || policy == CachePolicy.CACHE_ONLY;
    }

    String withVersionPrefix(String key, PipelineContext context) {
        if (key == null || context == null) {
            return key;
        }
        String versionTag = context.versionTag();
        if (versionTag == null || versionTag.isBlank()) {
            return key;
        }
        return versionTag + ":" + key;
    }

    PipelineCacheReader reader() {
        return reader;
    }
}
