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

/**
 * Cache policy applied by the cache plugin and pipeline cache support.
 */
public enum CachePolicy {
    /** Always read from cache and never invoke the underlying step. */
    CACHE_ONLY,
    /** Return cached value when present, otherwise continue pipeline execution. */
    RETURN_CACHED,
    /** Skip caching if the value already exists. */
    SKIP_IF_PRESENT,
    /** Require a cache hit and fail if missing. */
    REQUIRE_CACHE,
    /** Bypass cache reads/writes for this request. */
    BYPASS_CACHE;

    /**
     * Resolve a cache policy from a configuration value.
     *
     * @param value the configured policy string
     * @return the resolved cache policy, defaulting to RETURN_CACHED
     */
    public static CachePolicy fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return RETURN_CACHED;
        }
        String normalized = value.trim().replace('-', '_').toUpperCase();
        if ("PREFER_CACHE".equals(normalized)) {
            return RETURN_CACHED;
        }
        for (CachePolicy policy : values()) {
            if (policy.name().equals(normalized)) {
                return policy;
            }
        }
        return RETURN_CACHED;
    }
}
