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

    public ConnectorIdempotencyWindow(int maxKeys) {
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("maxKeys must be > 0");
        }
        this.maxKeys = maxKeys;
        this.seen = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > ConnectorIdempotencyWindow.this.maxKeys;
            }
        };
    }

    public synchronized boolean markIfNew(String key) {
        validate(key);
        return seen.put(key, Boolean.TRUE) == null;
    }

    public synchronized boolean contains(String key) {
        validate(key);
        return seen.containsKey(key);
    }

    synchronized int size() {
        return seen.size();
    }

    synchronized Map<String, Boolean> snapshot() {
        return Map.copyOf(seen);
    }

    private static void validate(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }
}
