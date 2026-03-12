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

    /**
     * Create a ConnectorIdempotencyTracker with a bounded accepted-key window.
     *
     * @param maxKeys the maximum number of accepted keys retained in the idempotency window
     */
    public ConnectorIdempotencyTracker(int maxKeys) {
        this.accepted = new ConnectorIdempotencyWindow(maxKeys);
    }

    /**
     * Attempt to reserve or record an idempotency key according to the given policy.
     *
     * @param key    the idempotency key to check or reserve; must be non-null and not blank unless {@code policy} is {@code DISABLED}
     * @param policy the idempotency policy that controls behavior:
     *               {@code DISABLED} always succeeds,
     *               {@code PRE_FORWARD} marks unseen keys as accepted,
     *               {@code ON_ACCEPT} reserves the key as in-flight if it is not already accepted or reserved
     * @return       {@code true} if the key was accepted or reserved according to the policy, {@code false} otherwise
     */
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

    /**
     * Records the given key as accepted when using the ON_ACCEPT idempotency policy and removes any in-flight reservation for it.
     *
     * If the provided policy is not {@code ConnectorIdempotencyPolicy.ON_ACCEPT}, this method does nothing.
     *
     * @param key    the connector handoff key to mark as accepted; must be non-null and not blank when the policy requires validation
     * @param policy the idempotency policy controlling behavior; must not be null
     * @throws NullPointerException     if {@code policy} is null or if {@code key} is null when {@code policy} is not {@code ConnectorIdempotencyPolicy.DISABLED}
     * @throws IllegalArgumentException if {@code key} is blank when {@code policy} is not {@code ConnectorIdempotencyPolicy.DISABLED}
     */
    public synchronized void markAccepted(String key, ConnectorIdempotencyPolicy policy) {
        validate(key, policy);
        if (policy != ConnectorIdempotencyPolicy.ON_ACCEPT) {
            return;
        }
        inFlight.remove(key);
        accepted.markIfNew(key);
    }

    /**
     * Removes all current in-flight reservations, releasing any held connector keys.
     */
    public synchronized void clearReservations() {
        inFlight.clear();
    }

    /**
     * Check whether the specified connector idempotency key has been recorded as accepted.
     *
     * @param key the connector idempotency key to check; must be non-null and not blank
     * @return `true` if the key is present in the accepted window, `false` otherwise
     * @throws NullPointerException     if {@code key} is null
     * @throws IllegalArgumentException if {@code key} is blank
     */
    public synchronized boolean containsAccepted(String key) {
        validateKey(key);
        return accepted.contains(key);
    }

    /**
     * Checks whether the given connector handoff key is currently reserved as in-flight.
     *
     * @param key the connector handoff key to check
     * @return `true` if the key is present in the in-flight set, `false` otherwise
     * @throws NullPointerException if `key` is null
     * @throws IllegalArgumentException if `key` is blank
     */
    public synchronized boolean containsInFlight(String key) {
        validateKey(key);
        return inFlight.contains(key);
    }

    /**
     * Validates the idempotency policy and, unless the policy is DISABLED, validates the provided key.
     *
     * @param key    the idempotency key to validate (must be non-null and not blank when validated)
     * @param policy the idempotency policy to check (must be non-null)
     * @throws NullPointerException     if {@code policy} is null or if {@code key} is null when validation is performed
     * @throws IllegalArgumentException if {@code key} is blank when validation is performed
     */
    private static void validate(String key, ConnectorIdempotencyPolicy policy) {
        Objects.requireNonNull(policy, "policy must not be null");
        if (policy == ConnectorIdempotencyPolicy.DISABLED) {
            return;
        }
        validateKey(key);
    }

    /**
     * Validate that the provided key is non-null and contains at least one non-whitespace character.
     *
     * @param key the key to validate; must be non-null and not blank
     * @throws NullPointerException if {@code key} is null
     * @throws IllegalArgumentException if {@code key} is blank (only whitespace)
     */
    private static void validateKey(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }
}
