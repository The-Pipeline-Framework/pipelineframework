/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.connector;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded in-memory LRU window for connector idempotency keys.
 */
public final class ConnectorIdempotencyWindow {

    private final int maxKeys;
    private final LinkedHashMap<String, Boolean> seen;

    /**
     * Creates a new idempotency window that keeps at most {@code maxKeys} distinct keys and evicts
     * the least-recently-used key when the capacity is exceeded.
     *
     * @param maxKeys the maximum number of keys to retain; must be greater than zero
     * @throws IllegalArgumentException if {@code maxKeys} is less than or equal to zero
     */
    public ConnectorIdempotencyWindow(int maxKeys) {
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("maxKeys must be > 0");
        }
        this.maxKeys = maxKeys;
        this.seen = new LinkedHashMap<>(16, 0.75f, true) {
            /**
             * Decides whether the eldest entry should be evicted when the map exceeds the configured maximum number of keys.
             *
             * @param eldest the eldest map entry (candidate for removal)
             * @return `true` if the map's size is greater than the configured maximum, `false` otherwise
             */
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > ConnectorIdempotencyWindow.this.maxKeys;
            }
        };
    }

    /**
     * Record the given idempotency key and indicate whether it was newly added.
     *
     * @param key the idempotency key to record; must not be null or blank
     * @return `true` if the key was not previously present and was added, `false` if it was already present
     * @throws NullPointerException     if `key` is null
     * @throws IllegalArgumentException if `key` is blank
     */
    public synchronized boolean markIfNew(String key) {
        validate(key);
        return seen.put(key, Boolean.TRUE) == null;
    }

    /**
     * Checks whether the given idempotency key is present in the window.
     *
     * @param key the idempotency key to check; must not be {@code null} or blank
     * @return {@code true} if the key is present in the window, {@code false} otherwise
     * @throws NullPointerException if {@code key} is {@code null}
     * @throws IllegalArgumentException if {@code key} is blank
     */
    public synchronized boolean contains(String key) {
        validate(key);
        return seen.containsKey(key);
    }

    /**
     * Gets the number of keys currently tracked in the idempotency window.
     *
     * @return the current number of entries in the window
     */
    synchronized int size() {
        return seen.size();
    }

    /**
     * Create an immutable snapshot of the current idempotency keys and their presence markers.
     *
     * @return an unmodifiable map containing the keys currently tracked and their boolean markers; the map reflects the state at the time of the call
     */
    synchronized Map<String, Boolean> snapshot() {
        return Map.copyOf(seen);
    }

    /**
     * Validate that the provided idempotency key is neither null nor blank.
     *
     * @param key the idempotency key to validate
     * @throws NullPointerException     if {@code key} is null
     * @throws IllegalArgumentException if {@code key} contains only whitespace
     */
    private static void validate(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }
}
