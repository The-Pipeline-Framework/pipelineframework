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

import java.time.Duration;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Abstraction for cache operations across different cache backends.
 *
 * @param <T> The type of items handled by this cache provider
 */
public interface CacheProvider<T> {

    /**
     * Gets the type of items handled by this cache provider.
     *
     * @return The Class object representing the handled type
     */
    Class<T> type();

    /**
     * Store the item in the cache using the supplied key.
     *
     * @param key cache key
     * @param value item to cache
     * @return a Uni that completes with the cached item
     */
    Uni<T> cache(String key, T value);

    /**
     * Store the item in the cache using the supplied key and optional TTL.
     *
     * @param key cache key
     * @param value item to cache
     * @param ttl time to live for the entry; null or non-positive values imply no TTL
     * @return a Uni that completes with the cached item
     */
    default Uni<T> cache(String key, T value, Duration ttl) {
        return cache(key, value);
    }

    /**
     * Retrieve a cached item by key if supported.
     *
     * @param key cache key
     * @return an Optional containing the cached item if present
     */
    default Uni<Optional<T>> get(String key) {
        return Uni.createFrom().item(Optional.empty());
    }

    /**
     * Determine if the cache contains the given key.
     *
     * @param key cache key
     * @return true if the key exists, false otherwise
     */
    default Uni<Boolean> exists(String key) {
        return get(key).map(Optional::isPresent);
    }

    /**
     * Invalidate the cache entry for the given key if supported.
     *
     * @param key cache key
     * @return true if the entry was invalidated, false otherwise
     */
    default Uni<Boolean> invalidate(String key) {
        return Uni.createFrom().item(false);
    }

    /**
     * Invalidate cache entries that match a prefix if supported.
     *
     * @param prefix key prefix to invalidate
     * @return true if entries were invalidated, false otherwise
     */
    default Uni<Boolean> invalidateByPrefix(String prefix) {
        return Uni.createFrom().item(false);
    }

    /**
     * Identifies the backend name for this provider.
     *
     * @return the backend identifier, or null if unspecified
     */
    default String backend() {
        return null;
    }

    /**
     * Determine whether this provider can handle the given item instance.
     *
     * @param item the item instance to evaluate; its runtime type is used to decide compatibility
     * @return {@code true} if the provider can cache the item's runtime type, {@code false} otherwise
     */
    boolean supports(Object item);

    /**
     * Indicates whether the provider supports the current thread context (virtual or platform threads).
     *
     * <p>Default implementation returns {@code true} to preserve backward compatibility.</p>
     *
     * @return {@code true} if the provider supports the current thread context, {@code false} otherwise
     */
    default boolean supportsThreadContext() {
        return true;
    }

    /**
     * Indicates whether this provider is safe to invoke concurrently.
     *
     * <p>Default implementation returns {@code SAFE} to preserve backward compatibility.</p>
     *
     * @return the thread safety declaration for this provider
     */
    default ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }
}
