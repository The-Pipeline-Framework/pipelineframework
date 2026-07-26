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

package org.pipelineframework.csv.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.blocking.CloseableIterator;
import org.pipelineframework.csv.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.domain.PaymentRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessCsvPaymentsInputServiceTest {

    @TempDir Path tempDir;

    private Path tempCsvFile;

    private ProcessCsvPaymentsInputService service;

    @BeforeEach
    void setUp() throws IOException {
        tempCsvFile = tempDir.resolve("test.csv");
        String csvContent =
                "ID,Recipient,Amount,Currency\n"
                        + UUID.randomUUID()
                        + ",John Doe,100.00,USD\n"
                        + UUID.randomUUID()
                        + ",Jane Smith,200.50,EUR\n";
        Files.writeString(tempCsvFile, csvContent);
        MockitoAnnotations.openMocks(this);
        service = new ProcessCsvPaymentsInputService();
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(tempCsvFile);
    }

    @Test
    void iterateBlocking() throws Exception {
        // Given
        CsvPaymentsInputFile csvFile = inputFile(tempCsvFile);

        // When
        List<PaymentRecord> records;
        try (CloseableIterator<PaymentRecord> iterator = service.iterateBlocking(csvFile)) {
            records = new java.util.ArrayList<>();
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
        }

        // Then
        assertEquals(2, records.size());

        PaymentRecord record1 = records.getFirst();
        assertNotNull(record1.id());
        assertNotNull(record1.csvId());
        assertEquals("John Doe", record1.recipient());
        assertEquals(new BigDecimal("100.00"), record1.amount());
        assertEquals(Currency.getInstance("USD"), record1.currency());
        assertEquals(csvFile.filepath(), record1.csvPaymentsInputFilePath());

        PaymentRecord record2 = records.get(1);
        assertNotNull(record2.id());
        assertNotNull(record2.csvId());
        assertEquals("Jane Smith", record2.recipient());
        assertEquals(new BigDecimal("200.50"), record2.amount());
        assertEquals(Currency.getInstance("EUR"), record2.currency());
        assertEquals(csvFile.filepath(), record2.csvPaymentsInputFilePath());

        List<PaymentRecord> rereadRecords;
        try (CloseableIterator<PaymentRecord> iterator = service.iterateBlocking(csvFile)) {
            rereadRecords = new java.util.ArrayList<>();
            while (iterator.hasNext()) {
                rereadRecords.add(iterator.next());
            }
        }
        assertEquals(records.stream().map(PaymentRecord::id).toList(),
            rereadRecords.stream().map(PaymentRecord::id).toList());
        assertEquals(records.stream().map(PaymentRecord::csvId).toList(),
            rereadRecords.stream().map(PaymentRecord::csvId).toList());
    }

    @Test
    void iterateBlockingReturnsNonEmptyIterator() throws Exception {
        CsvPaymentsInputFile csvFile = inputFile(tempCsvFile);

        try (CloseableIterator<PaymentRecord> iterator = service.iterateBlocking(csvFile)) {
            assertNotNull(iterator);
            assertTrue(iterator.hasNext());
        }
    }

    @Test
    @SneakyThrows
    void process_fileNotFound() {
        CsvPaymentsInputFile csvFile =
                inputFile(tempDir.resolve("nonexistent.csv"));
        assertThrows(RuntimeException.class, () -> service.iterateBlocking(csvFile));
    }

    @Test
    void process_invalidCsvContent() throws IOException {
        // Given
        String invalidCsvContent =
                "ID,Recipient,Amount,Currency\n"
                        + UUID.randomUUID()
                        + ",John Doe,invalid_amount,USD\n";
        Files.writeString(tempCsvFile, invalidCsvContent);
        CsvPaymentsInputFile csvFile = inputFile(tempCsvFile);

        // When
        assertThrows(RuntimeException.class, () -> {
            try (CloseableIterator<PaymentRecord> iterator = service.iterateBlocking(csvFile)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
    }

    @Test
    void processReactiveAdapterStreamsRecords() {
        CsvPaymentsInputFile csvFile = inputFile(tempCsvFile);

        List<PaymentRecord> records = service.process(csvFile)
            .collect()
            .asList()
            .await()
            .indefinitely();

        assertEquals(2, records.size());
    }

    private CsvPaymentsInputFile inputFile(Path path) {
        return new CsvPaymentsInputFile(path, path.getParent());
    }
}
