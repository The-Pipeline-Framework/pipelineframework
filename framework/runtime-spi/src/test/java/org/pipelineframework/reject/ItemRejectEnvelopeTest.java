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

package org.pipelineframework.reject;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ItemRejectEnvelopeTest {

    @Test
    void requiresStepClass() {
        assertThrows(NullPointerException.class, () -> envelope(null, "Step", "ITEM", "Failure", "fingerprint", 1L));
    }

    @Test
    void requiresStepName() {
        assertThrows(NullPointerException.class, () -> envelope("example.Step", null, "ITEM", "Failure", "fingerprint", 1L));
    }

    @Test
    void requiresRejectScope() {
        assertThrows(NullPointerException.class, () -> envelope("example.Step", "Step", null, "Failure", "fingerprint", 1L));
    }

    @Test
    void requiresErrorClass() {
        assertThrows(NullPointerException.class, () -> envelope("example.Step", "Step", "ITEM", null, "fingerprint", 1L));
    }

    @Test
    void requiresItemFingerprint() {
        assertThrows(NullPointerException.class, () -> envelope("example.Step", "Step", "ITEM", "Failure", null, 1L));
    }

    @Test
    void requiresPositiveTimestamp() {
        assertThrows(IllegalArgumentException.class, () -> envelope("example.Step", "Step", "ITEM", "Failure", "fingerprint", 0L));
        assertThrows(IllegalArgumentException.class, () -> envelope("example.Step", "Step", "ITEM", "Failure", "fingerprint", -1L));
    }

    @Test
    void preservesAllRecordFields() {
        Object payload = new Object();
        ItemRejectEnvelope envelope = new ItemRejectEnvelope(
            "tenant-1",
            "execution-1",
            "correlation-1",
            "idempotency-1",
            "replay",
            "example.Step",
            "Step",
            "STREAM",
            2,
            3,
            4,
            5,
            "example.Failure",
            "failed",
            1_700_000_000_000L,
            "fingerprint-1",
            6L,
            payload);

        assertEquals("tenant-1", envelope.tenantId());
        assertEquals("execution-1", envelope.executionId());
        assertEquals("correlation-1", envelope.correlationId());
        assertEquals("idempotency-1", envelope.idempotencyKey());
        assertEquals("replay", envelope.replayMode());
        assertEquals("example.Step", envelope.stepClass());
        assertEquals("Step", envelope.stepName());
        assertEquals("STREAM", envelope.rejectScope());
        assertEquals(2, envelope.transportRetryAttempt());
        assertEquals(3, envelope.retriesObserved());
        assertEquals(4, envelope.retryLimit());
        assertEquals(5, envelope.finalAttempt());
        assertEquals("example.Failure", envelope.errorClass());
        assertEquals("failed", envelope.errorMessage());
        assertEquals(1_700_000_000_000L, envelope.rejectedAtEpochMs());
        assertEquals("fingerprint-1", envelope.itemFingerprint());
        assertEquals(6L, envelope.itemCount());
        assertSame(payload, envelope.payload());
    }

    private static ItemRejectEnvelope envelope(
        String stepClass,
        String stepName,
        String rejectScope,
        String errorClass,
        String itemFingerprint,
        long rejectedAtEpochMs
    ) {
        return new ItemRejectEnvelope(
            null,
            null,
            null,
            null,
            null,
            stepClass,
            stepName,
            rejectScope,
            null,
            null,
            null,
            null,
            errorClass,
            null,
            rejectedAtEpochMs,
            itemFingerprint,
            null,
            null);
    }
}
