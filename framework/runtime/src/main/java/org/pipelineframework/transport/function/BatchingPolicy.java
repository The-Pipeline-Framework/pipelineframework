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

package org.pipelineframework.transport.function;

import java.time.Duration;
import java.util.Objects;

/**
 * Boundary batching controls for function transport adapters.
 *
 * <p>This policy applies only at ingress/egress boundaries. Inside the pipeline, TPF still uses
 * reactive {@code Multi}/{@code Uni} with normal backpressure behavior.</p>
 *
 * @param maxItems maximum number of items in a batch
 * @param maxBytes maximum serialized bytes in a batch
 * @param maxWait maximum wait time before flushing a partial batch
 * @param maxInFlight maximum concurrent in-flight batches
 * @param overflowPolicy overflow behavior when limits are reached
 */
public record BatchingPolicy(
    int maxItems,
    int maxBytes,
    Duration maxWait,
    int maxInFlight,
    BatchOverflowPolicy overflowPolicy
) {
    private static final int DEFAULT_MAX_ITEMS = 128;
    private static final int DEFAULT_MAX_BYTES = 1024 * 1024;
    private static final Duration DEFAULT_MAX_WAIT = Duration.ofMillis(250);
    private static final int DEFAULT_MAX_IN_FLIGHT = 16;

    /**
     * Creates a validated batching policy.
     *
     * @throws IllegalArgumentException when numeric limits are non-positive
     * @throws NullPointerException when maxWait or overflowPolicy is null
     */
    public BatchingPolicy {
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be > 0");
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be > 0");
        }
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be > 0");
        }
        Objects.requireNonNull(maxWait, "maxWait is required");
        Objects.requireNonNull(overflowPolicy, "overflowPolicy is required");
    }

    /**
     * Default policy tuned for low-latency reactive function boundaries.
     *
     * @return default batching policy
     */
    public static BatchingPolicy defaultPolicy() {
        return new BatchingPolicy(
            DEFAULT_MAX_ITEMS,
            DEFAULT_MAX_BYTES,
            DEFAULT_MAX_WAIT,
            DEFAULT_MAX_IN_FLIGHT,
            BatchOverflowPolicy.BUFFER);
    }
}

