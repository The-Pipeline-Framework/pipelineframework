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

package org.pipelineframework.context;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TransportDispatchMetadataHolderTest {

    @AfterEach
    void cleanup() {
        TransportDispatchMetadataHolder.clear();
    }

    @Test
    void returnsNullWhenNotSet() {
        assertNull(TransportDispatchMetadataHolder.get());
    }

    @Test
    void storesAndRetrievesMetadata() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            "corr-1", "exec-1", "idem-1", 0, null, null, null);

        TransportDispatchMetadataHolder.set(metadata);

        TransportDispatchMetadata retrieved = TransportDispatchMetadataHolder.get();
        assertEquals("corr-1", retrieved.correlationId());
        assertEquals("exec-1", retrieved.executionId());
        assertEquals("idem-1", retrieved.idempotencyKey());
    }

    @Test
    void overwritesPreviousMetadata() {
        TransportDispatchMetadata first = new TransportDispatchMetadata(
            "corr-1", "exec-1", "idem-1", 0, null, null, null);
        TransportDispatchMetadata second = new TransportDispatchMetadata(
            "corr-2", "exec-2", "idem-2", 1, null, null, null);

        TransportDispatchMetadataHolder.set(first);
        TransportDispatchMetadataHolder.set(second);

        TransportDispatchMetadata retrieved = TransportDispatchMetadataHolder.get();
        assertEquals("corr-2", retrieved.correlationId());
        assertEquals("exec-2", retrieved.executionId());
    }

    @Test
    void clearsMetadata() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            "corr-1", "exec-1", "idem-1", 0, null, null, null);

        TransportDispatchMetadataHolder.set(metadata);
        TransportDispatchMetadataHolder.clear();

        assertNull(TransportDispatchMetadataHolder.get());
    }

    @Test
    void handlesNullMetadata() {
        TransportDispatchMetadataHolder.set(null);
        assertNull(TransportDispatchMetadataHolder.get());
    }

    @Test
    void clearIsIdempotent() {
        TransportDispatchMetadataHolder.clear();
        TransportDispatchMetadataHolder.clear();
        assertNull(TransportDispatchMetadataHolder.get());
    }

    @Test
    void isolatesMetadataAcrossThreads() throws InterruptedException {
        TransportDispatchMetadata mainMetadata = new TransportDispatchMetadata(
            "main-corr", "main-exec", "main-idem", 0, null, null, null);
        TransportDispatchMetadataHolder.set(mainMetadata);

        Thread thread = new Thread(() -> {
            assertNull(TransportDispatchMetadataHolder.get());
            TransportDispatchMetadata threadMetadata = new TransportDispatchMetadata(
                "thread-corr", "thread-exec", "thread-idem", 1, null, null, null);
            TransportDispatchMetadataHolder.set(threadMetadata);

            TransportDispatchMetadata retrieved = TransportDispatchMetadataHolder.get();
            assertEquals("thread-corr", retrieved.correlationId());
        });

        thread.start();
        thread.join();

        TransportDispatchMetadata mainRetrieved = TransportDispatchMetadataHolder.get();
        assertEquals("main-corr", mainRetrieved.correlationId());
    }

    @Test
    void storesCompleteMetadataWithAllFields() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            "corr-full",
            "exec-full",
            "idem-full",
            5,
            2000000000000L,
            1999999999999L,
            "parent-full");

        TransportDispatchMetadataHolder.set(metadata);

        TransportDispatchMetadata retrieved = TransportDispatchMetadataHolder.get();
        assertEquals("corr-full", retrieved.correlationId());
        assertEquals("exec-full", retrieved.executionId());
        assertEquals("idem-full", retrieved.idempotencyKey());
        assertEquals(5, retrieved.retryAttempt());
        assertEquals(2000000000000L, retrieved.deadlineEpochMs());
        assertEquals(1999999999999L, retrieved.dispatchTsEpochMs());
        assertEquals("parent-full", retrieved.parentItemId());
    }
}