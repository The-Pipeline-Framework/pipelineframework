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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ItemRejectSinkTest {

    @Test
    void defaultsArePortableNotDurableAndFailClosedWithoutReadinessOverride() {
        ItemRejectSink sink = envelope -> Uni.createFrom().voidItem();

        assertEquals("log", sink.providerName());
        assertEquals(-500, sink.priority());
        assertFalse(sink.durable());
        IllegalStateException error = assertThrows(IllegalStateException.class, sink::startupValidationError);
        assertTrue(error.getMessage().contains("must implement startupValidationError()"));
    }

    @Test
    void providersCanOverrideSelectionDurabilityAndReadiness() {
        ItemRejectSink sink = new ItemRejectSink() {
            @Override
            public String providerName() {
                return "archive";
            }

            @Override
            public int priority() {
                return 25;
            }

            @Override
            public boolean durable() {
                return true;
            }

            @Override
            public Optional<String> startupValidationError() {
                return Optional.of("archive endpoint is unavailable");
            }

            @Override
            public Uni<Void> publish(ItemRejectEnvelope envelope) {
                return Uni.createFrom().voidItem();
            }
        };

        assertEquals("archive", sink.providerName());
        assertEquals(25, sink.priority());
        assertTrue(sink.durable());
        assertEquals(Optional.of("archive endpoint is unavailable"), sink.startupValidationError());
    }

    @Test
    void publishCompletesWithoutAnItemAndReceivesTheEnvelope() {
        AtomicReference<ItemRejectEnvelope> published = new AtomicReference<>();
        ItemRejectSink sink = envelope -> Uni.createFrom().voidItem().invoke(() -> published.set(envelope));
        ItemRejectEnvelope envelope = sampleEnvelope();

        Void result = sink.publish(envelope).await().indefinitely();

        assertNull(result);
        assertSame(envelope, published.get());
    }

    private static ItemRejectEnvelope sampleEnvelope() {
        return new ItemRejectEnvelope(
            null,
            "execution-1",
            "correlation-1",
            "idempotency-1",
            "none",
            "example.Step",
            "Step",
            "ITEM",
            0,
            0,
            3,
            1,
            "example.Failure",
            "failed",
            1_700_000_000_000L,
            "fingerprint-1",
            null,
            null);
    }
}
