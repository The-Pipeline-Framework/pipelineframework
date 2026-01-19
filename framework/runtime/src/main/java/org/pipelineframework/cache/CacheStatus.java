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
 * Cache status values reported by cache plugin steps.
 */
public enum CacheStatus {
    /** Cache hit returned a stored value. */
    HIT,
    /** Cache miss returned no stored value. */
    MISS,
    /** Cache was bypassed for this request. */
    BYPASS,
    /** Cache write occurred for this request. */
    WRITE;

    /**
     * Parse a cache status from a header value.
     *
     * @param value the header value
     * @return the resolved cache status, or null when unset or invalid
     */
    public static CacheStatus fromHeader(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String normalized = value.trim().toUpperCase();
        for (CacheStatus status : values()) {
            if (status.name().equals(normalized)) {
                return status;
            }
        }
        return null;
    }
}
