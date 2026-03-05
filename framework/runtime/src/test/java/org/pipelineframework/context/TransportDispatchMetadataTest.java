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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransportDispatchMetadataTest {

    @Test
    void createsMetadataWithAllFields() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            "corr-123",
            "exec-456",
            "idem-789",
            3,
            2000000000000L,
            1999999999999L,
            "parent-abc");

        assertEquals("corr-123", metadata.correlationId());
        assertEquals("exec-456", metadata.executionId());
        assertEquals("idem-789", metadata.idempotencyKey());
        assertEquals(3, metadata.retryAttempt());
        assertEquals(2000000000000L, metadata.deadlineEpochMs());
        assertEquals(1999999999999L, metadata.dispatchTsEpochMs());
        assertEquals("parent-abc", metadata.parentItemId());
    }

    @Test
    void allowsNullFields() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            null, null, null, null, null, null, null);

        assertNull(metadata.correlationId());
        assertNull(metadata.executionId());
        assertNull(metadata.idempotencyKey());
        assertNull(metadata.retryAttempt());
        assertNull(metadata.deadlineEpochMs());
        assertNull(metadata.dispatchTsEpochMs());
        assertNull(metadata.parentItemId());
    }

    @Test
    void rejectsNegativeRetryAttempt() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new TransportDispatchMetadata("c", "e", "i", -1, null, null, null));
        assertEquals("retryAttempt must be >= 0", ex.getMessage());
    }

    @Test
    void rejectsNegativeDeadlineEpochMs() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new TransportDispatchMetadata("c", "e", "i", null, -1L, null, null));
        assertEquals("deadlineEpochMs must be >= 0", ex.getMessage());
    }

    @Test
    void rejectsNegativeDispatchTsEpochMs() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new TransportDispatchMetadata("c", "e", "i", null, null, -1L, null));
        assertEquals("dispatchTsEpochMs must be >= 0", ex.getMessage());
    }

    @Test
    void acceptsZeroRetryAttempt() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            "c", "e", "i", 0, null, null, null);
        assertEquals(0, metadata.retryAttempt());
    }

    @Test
    void acceptsZeroDeadlineEpochMs() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            "c", "e", "i", null, 0L, null, null);
        assertEquals(0L, metadata.deadlineEpochMs());
    }

    @Test
    void acceptsZeroDispatchTsEpochMs() {
        TransportDispatchMetadata metadata = new TransportDispatchMetadata(
            "c", "e", "i", null, null, 0L, null);
        assertEquals(0L, metadata.dispatchTsEpochMs());
    }

    @Test
    void parsesValidHeadersWithFromHeaders() {
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "corr-abc",
            "exec-def",
            "idem-ghi",
            "5",
            "1700000000000",
            "1699999999999",
            "parent-xyz");

        assertEquals("corr-abc", metadata.correlationId());
        assertEquals("exec-def", metadata.executionId());
        assertEquals("idem-ghi", metadata.idempotencyKey());
        assertEquals(5, metadata.retryAttempt());
        assertEquals(1700000000000L, metadata.deadlineEpochMs());
        assertEquals(1699999999999L, metadata.dispatchTsEpochMs());
        assertEquals("parent-xyz", metadata.parentItemId());
    }

    @Test
    void trimsWhitespaceFromHeaderValues() {
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "  corr-123  ",
            "  exec-456  ",
            "  idem-789  ",
            "  2  ",
            "  1600000000000  ",
            "  1599999999999  ",
            "  parent-id  ");

        assertEquals("corr-123", metadata.correlationId());
        assertEquals("exec-456", metadata.executionId());
        assertEquals("idem-789", metadata.idempotencyKey());
        assertEquals(2, metadata.retryAttempt());
        assertEquals(1600000000000L, metadata.deadlineEpochMs());
        assertEquals(1599999999999L, metadata.dispatchTsEpochMs());
        assertEquals("parent-id", metadata.parentItemId());
    }

    @Test
    void convertsBlankHeadersToNull() {
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "   ",
            "",
            null,
            "   ",
            "",
            null,
            "   ");

        assertNull(metadata.correlationId());
        assertNull(metadata.executionId());
        assertNull(metadata.idempotencyKey());
        assertNull(metadata.retryAttempt());
        assertNull(metadata.deadlineEpochMs());
        assertNull(metadata.dispatchTsEpochMs());
        assertNull(metadata.parentItemId());
    }

    @Test
    void ignoresInvalidIntegerHeader() {
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "corr", "exec", "idem", "not-a-number", null, null, null);

        assertEquals("corr", metadata.correlationId());
        assertEquals("exec", metadata.executionId());
        assertEquals("idem", metadata.idempotencyKey());
        assertNull(metadata.retryAttempt());
    }

    @Test
    void ignoresInvalidLongHeaders() {
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "corr", "exec", "idem", null, "invalid-long", "also-invalid", null);

        assertEquals("corr", metadata.correlationId());
        assertEquals("exec", metadata.executionId());
        assertEquals("idem", metadata.idempotencyKey());
        assertNull(metadata.deadlineEpochMs());
        assertNull(metadata.dispatchTsEpochMs());
    }

    @Test
    void handlesPartiallyValidHeaders() {
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "corr-valid",
            "",
            "idem-valid",
            "3",
            "invalid",
            "1650000000000",
            null);

        assertEquals("corr-valid", metadata.correlationId());
        assertNull(metadata.executionId());
        assertEquals("idem-valid", metadata.idempotencyKey());
        assertEquals(3, metadata.retryAttempt());
        assertNull(metadata.deadlineEpochMs());
        assertEquals(1650000000000L, metadata.dispatchTsEpochMs());
        assertNull(metadata.parentItemId());
    }

    @Test
    void handlesZeroValuesInFromHeaders() {
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "corr", "exec", "idem", "0", "0", "0", "parent");

        assertEquals(0, metadata.retryAttempt());
        assertEquals(0L, metadata.deadlineEpochMs());
        assertEquals(0L, metadata.dispatchTsEpochMs());
    }

    @Test
    void handlesMaximumLongValues() {
        String maxLong = String.valueOf(Long.MAX_VALUE);
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "corr", "exec", "idem", null, maxLong, maxLong, null);

        assertEquals(Long.MAX_VALUE, metadata.deadlineEpochMs());
        assertEquals(Long.MAX_VALUE, metadata.dispatchTsEpochMs());
    }

    @Test
    void handlesMaximumIntegerValue() {
        String maxInt = String.valueOf(Integer.MAX_VALUE);
        TransportDispatchMetadata metadata = TransportDispatchMetadata.fromHeaders(
            "corr", "exec", "idem", maxInt, null, null, null);

        assertEquals(Integer.MAX_VALUE, metadata.retryAttempt());
    }
}