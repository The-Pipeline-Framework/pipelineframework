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

package org.pipelineframework.transport.function;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FunctionTransportContextTest {

    @Test
    void defaultsToContextStableWhenPolicyIsUnset() {
        FunctionTransportContext context = FunctionTransportContext.of("req-1", "fn", "ingress");
        assertEquals(IdempotencyPolicy.CONTEXT_STABLE, context.idempotencyPolicy());
    }

    @Test
    void mapsLegacyRandomAliasToContextStable() {
        FunctionTransportContext context = new FunctionTransportContext(
            "req-2",
            "fn",
            "ingress",
            Map.of(FunctionTransportContext.ATTR_IDEMPOTENCY_POLICY, "RANDOM"));
        assertEquals(IdempotencyPolicy.CONTEXT_STABLE, context.idempotencyPolicy());
    }

    @Test
    void fallsBackUnknownPolicyToContextStable() {
        FunctionTransportContext context = new FunctionTransportContext(
            "req-3",
            "fn",
            "ingress",
            Map.of(FunctionTransportContext.ATTR_IDEMPOTENCY_POLICY, "WHATEVER"));
        assertEquals(IdempotencyPolicy.CONTEXT_STABLE, context.idempotencyPolicy());
    }

    @ParameterizedTest
    @ValueSource(strings = {"explicit", "EXPLICIT", "Explicit"})
    void resolvesExplicitPolicyCaseInsensitively(String rawPolicy) {
        FunctionTransportContext context = new FunctionTransportContext(
            "req-5",
            "fn",
            "ingress",
            Map.of(FunctionTransportContext.ATTR_IDEMPOTENCY_POLICY, rawPolicy));
        assertEquals(IdempotencyPolicy.EXPLICIT, context.idempotencyPolicy());
    }
}
