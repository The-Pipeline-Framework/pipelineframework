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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchingPolicyTest {

    @Test
    void createsDefaultPolicy() {
        BatchingPolicy policy = BatchingPolicy.defaultPolicy();
        assertEquals(128, policy.maxItems());
        assertEquals(1_048_576, policy.maxBytes());
        assertEquals(Duration.ofMillis(250), policy.maxWait());
        assertEquals(16, policy.maxInFlight());
        assertEquals(BatchOverflowPolicy.BUFFER, policy.overflowPolicy());
    }

    @Test
    void rejectsNonPositiveMaxItems() {
        assertThrows(IllegalArgumentException.class, () ->
            new BatchingPolicy(0, 1024, Duration.ofMillis(100), 1, BatchOverflowPolicy.BUFFER));
    }

    @Test
    void rejectsNonPositiveMaxBytes() {
        assertThrows(IllegalArgumentException.class, () ->
            new BatchingPolicy(1, 0, Duration.ofMillis(100), 1, BatchOverflowPolicy.BUFFER));
    }

    @Test
    void rejectsNonPositiveMaxWait() {
        assertThrows(IllegalArgumentException.class, () ->
            new BatchingPolicy(1, 1024, Duration.ZERO, 1, BatchOverflowPolicy.BUFFER));
    }

    @Test
    void rejectsNullOverflowPolicy() {
        assertThrows(NullPointerException.class, () ->
            new BatchingPolicy(1, 1024, Duration.ofMillis(100), 1, null));
    }
}
