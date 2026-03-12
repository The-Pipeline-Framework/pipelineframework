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

/**
 * Runtime policy applied to a connector handoff.
 *
 * @param enabled whether connector startup should create a live subscription
 * @param backpressurePolicy overflow behavior at the handoff boundary
 * @param backpressureBufferCapacity buffer size when BUFFER mode is active
 * @param idempotencyPolicy duplicate handling mode
 * @param failureMode mapping-failure behavior
 */
public record ConnectorPolicy(
    boolean enabled,
    ConnectorBackpressurePolicy backpressurePolicy,
    int backpressureBufferCapacity,
    ConnectorIdempotencyPolicy idempotencyPolicy,
    ConnectorFailureMode failureMode
) {
    public static final int DEFAULT_BACKPRESSURE_BUFFER_CAPACITY = 256;
    public static final int MAX_BACKPRESSURE_BUFFER_CAPACITY = 1_000_000;

    public ConnectorPolicy {
        backpressurePolicy = backpressurePolicy == null ? ConnectorBackpressurePolicy.BUFFER : backpressurePolicy;
        if (backpressureBufferCapacity <= 0) {
            backpressureBufferCapacity = DEFAULT_BACKPRESSURE_BUFFER_CAPACITY;
        } else if (backpressureBufferCapacity > MAX_BACKPRESSURE_BUFFER_CAPACITY) {
            throw new IllegalArgumentException(
                "backpressureBufferCapacity must be <= " + MAX_BACKPRESSURE_BUFFER_CAPACITY);
        }
        idempotencyPolicy = idempotencyPolicy == null ? ConnectorIdempotencyPolicy.DISABLED : idempotencyPolicy;
        failureMode = failureMode == null ? ConnectorFailureMode.PROPAGATE : failureMode;
    }

    public static ConnectorPolicy disabled() {
        return new ConnectorPolicy(
            false,
            ConnectorBackpressurePolicy.BUFFER,
            DEFAULT_BACKPRESSURE_BUFFER_CAPACITY,
            ConnectorIdempotencyPolicy.DISABLED,
            ConnectorFailureMode.PROPAGATE);
    }
}
