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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks seen and in-flight connector handoff keys.
 */
public final class ConnectorIdempotencyTracker {

    private final ConnectorIdempotencyWindow accepted;
    private final Set<String> inFlight = new HashSet<>();

    public ConnectorIdempotencyTracker(int maxKeys) {
        this.accepted = new ConnectorIdempotencyWindow(maxKeys);
    }

    public synchronized boolean tryAcquire(String key, ConnectorIdempotencyPolicy policy) {
        validate(key, policy);
        return switch (policy) {
            case DISABLED -> true;
            case PRE_FORWARD -> accepted.markIfNew(key);
            case ON_ACCEPT -> {
                if (accepted.contains(key) || inFlight.contains(key)) {
                    yield false;
                }
                inFlight.add(key);
                yield true;
            }
        };
    }

    public synchronized void markAccepted(String key, ConnectorIdempotencyPolicy policy) {
        validate(key, policy);
        if (policy != ConnectorIdempotencyPolicy.ON_ACCEPT) {
            return;
        }
        inFlight.remove(key);
        accepted.markIfNew(key);
    }

    public synchronized void clearReservations() {
        inFlight.clear();
    }

    public synchronized boolean containsAccepted(String key) {
        validateKey(key);
        return accepted.contains(key);
    }

    public synchronized boolean containsInFlight(String key) {
        validateKey(key);
        return inFlight.contains(key);
    }

    private static void validate(String key, ConnectorIdempotencyPolicy policy) {
        Objects.requireNonNull(policy, "policy must not be null");
        if (policy == ConnectorIdempotencyPolicy.DISABLED) {
            return;
        }
        validateKey(key);
    }

    private static void validateKey(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }
}
