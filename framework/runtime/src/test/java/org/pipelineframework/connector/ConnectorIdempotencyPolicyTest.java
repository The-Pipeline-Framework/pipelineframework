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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class ConnectorIdempotencyPolicyTest {

    @Test
    void enumContainsDisabledPolicy() {
        ConnectorIdempotencyPolicy policy = ConnectorIdempotencyPolicy.DISABLED;
        assertNotNull(policy);
        assertEquals("DISABLED", policy.name());
    }

    @Test
    void enumContainsPreForwardPolicy() {
        ConnectorIdempotencyPolicy policy = ConnectorIdempotencyPolicy.PRE_FORWARD;
        assertNotNull(policy);
        assertEquals("PRE_FORWARD", policy.name());
    }

    @Test
    void enumContainsOnAcceptPolicy() {
        ConnectorIdempotencyPolicy policy = ConnectorIdempotencyPolicy.ON_ACCEPT;
        assertNotNull(policy);
        assertEquals("ON_ACCEPT", policy.name());
    }

    @Test
    void valueOfReturnsCorrectPolicy() {
        assertEquals(ConnectorIdempotencyPolicy.DISABLED,
            ConnectorIdempotencyPolicy.valueOf("DISABLED"));
        assertEquals(ConnectorIdempotencyPolicy.PRE_FORWARD,
            ConnectorIdempotencyPolicy.valueOf("PRE_FORWARD"));
        assertEquals(ConnectorIdempotencyPolicy.ON_ACCEPT,
            ConnectorIdempotencyPolicy.valueOf("ON_ACCEPT"));
    }

    @Test
    void valuesReturnsAllPolicies() {
        ConnectorIdempotencyPolicy[] policies = ConnectorIdempotencyPolicy.values();
        assertEquals(3, policies.length);
    }
}