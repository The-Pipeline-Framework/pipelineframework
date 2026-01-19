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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.StringJoiner;

/**
 * Utility for producing stable cache key strings for pipeline caching.
 */
public final class PipelineCacheKeyFormat {

    private PipelineCacheKeyFormat() {
    }

    /**
     * Build a cache key for a single value without a version tag.
     *
     * @param value the value to normalize into a cache key
     * @return a stable key string
     */
    public static String baseKey(Object value) {
        if (value == null) {
            return "null";
        }
        return value.getClass().getName() + ":" + normalizeValue(value);
    }

    /**
     * Build a cache key for multiple values without a version tag.
     *
     * @param values method parameters to normalize
     * @return a stable key string
     */
    public static String baseKeyForParams(Object... values) {
        if (values == null || values.length == 0) {
            return "no-params";
        }
        StringJoiner joiner = new StringJoiner("|");
        for (Object value : values) {
            joiner.add(baseKey(value));
        }
        return joiner.toString();
    }

    /**
     * Prefix the key with a version tag if provided.
     *
     * @param baseKey the base key to prefix
     * @param versionTag the version tag; may be null or blank
     * @return the versioned key string
     */
    public static String applyVersionTag(String baseKey, String versionTag) {
        if (versionTag == null || versionTag.isBlank()) {
            return baseKey;
        }
        return versionTag + ":" + baseKey;
    }

    /**
     * Build a prefix suitable for invalidating all entries of a given type.
     *
     * @param type the value type
     * @param versionTag optional version tag
     * @return a prefix string including the trailing separator
     */
    public static String typePrefix(Class<?> type, String versionTag) {
        String prefix = type == null ? "unknown" : type.getName();
        String base = prefix + ":";
        return applyVersionTag(base, versionTag);
    }

    private static String normalizeValue(Object value) {
        if (value instanceof CacheKey cacheKey) {
            return cacheKey.cacheKey();
        }
        if (value instanceof com.google.protobuf.Message message) {
            return digestBytes(message.toByteArray());
        }
        return String.valueOf(value);
    }

    private static String digestBytes(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (Exception e) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
