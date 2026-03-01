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

package org.pipelineframework.csv.common.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PaymentRecordTest {

    @Test
    void testConstructor() {
        // When
        PaymentRecord record = new PaymentRecord();

        // Then
        assertNotNull(record.getId());
        assertInstanceOf(UUID.class, record.getId());
    }

    @Test
    void testGetSetCsvId() {
        // Given
        PaymentRecord record = new PaymentRecord();
        String csvId = "test-csv-id";

        // When
        record.setCsvId(csvId);

        // Then
        assertEquals(csvId, record.getCsvId());
    }

    @Test
    void testGetSetRecipient() {
        // Given
        PaymentRecord record = new PaymentRecord();
        String recipient = "Test Recipient";

        // When
        record.setRecipient(recipient);

        // Then
        assertEquals(recipient, record.getRecipient());
    }

    @Test
    void testGetSetAmount() {
        // Given
        PaymentRecord record = new PaymentRecord();
        BigDecimal amount = new BigDecimal("100.50");

        // When
        record.setAmount(amount);

        // Then
        assertEquals(amount, record.getAmount());
    }

    @Test
    void testGetSetCurrency() {
        // Given
        PaymentRecord record = new PaymentRecord();
        Currency currency = Currency.getInstance("EUR");

        // When
        record.setCurrency(currency);

        // Then
        assertEquals(currency, record.getCurrency());
    }

    @Test
    void testGetSetCsvPaymentsInputFilePath() {
        // Given
        PaymentRecord record = new PaymentRecord();
        Path path = Path.of("/test/path/file.csv");

        // When
        record.setCsvPaymentsInputFilePath(path);

        // Then
        assertEquals(path, record.getCsvPaymentsInputFilePath());
    }

    @Test
    void testToString() {
        // Given
        PaymentRecord record = new PaymentRecord();
        record.setCsvId("test-id");
        record.setRecipient("Test Recipient");
        record.setAmount(new BigDecimal("100.50"));
        record.setCurrency(Currency.getInstance("EUR"));
        record.setCsvPaymentsInputFilePath(Path.of("/test/path/file.csv"));

        // When
        String result = record.toString();

        // Then
        assertTrue(result.contains("test-id"));
        assertTrue(result.contains("Test Recipient"));
        assertTrue(result.contains("100.50"));
        assertTrue(result.contains("EUR"));
        assertTrue(result.contains("/test/path/file.csv"));
    }

    @Test
    void testEquals_SameObject() {
        // Given
        PaymentRecord record = new PaymentRecord();

        // When
        boolean result = record.equals(record);

        // Then
        assertTrue(result);
    }

    @Test
    void testEquals_NullObject() {
        // Given
        PaymentRecord record = new PaymentRecord();

        // When
        boolean result = record.equals(null);

        // Then
        assertFalse(result);
    }

    @Test
    void testEquals_DifferentClass() {
        // Given
        PaymentRecord record = new PaymentRecord();
        String other = "other";

        // When
        boolean result = record.equals(other);

        // Then
        assertFalse(result);
    }

    @Test
    void testEquals_SameId() {
        // Given
        PaymentRecord record1 = new PaymentRecord();
        PaymentRecord record2 = new PaymentRecord();
        UUID id = UUID.randomUUID();
        record1.setId(id);
        record2.setId(id);

        // When
        boolean result = record1.equals(record2);

        // Then
        assertTrue(result);
    }

    @Test
    void testEquals_DifferentId() {
        // Given
        PaymentRecord record1 = new PaymentRecord();
        PaymentRecord record2 = new PaymentRecord();

        // When
        boolean result = record1.equals(record2);

        // Then
        assertFalse(result);
    }

    @Test
    void testHashCode() {
        // Given
        PaymentRecord record = new PaymentRecord();
        UUID id = record.getId();

        // When
        int hashCode = record.hashCode();
        int expectedHashCode = java.util.Objects.hash(id);

        // Then
        assertEquals(expectedHashCode, hashCode);
    }
}
