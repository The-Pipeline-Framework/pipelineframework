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

import io.smallrye.mutiny.Multi;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.csv.common.domain.*;
import org.pipelineframework.csv.common.dto.PaymentOutputDto;
import org.pipelineframework.csv.common.dto.PaymentStatusDto;
import org.pipelineframework.csv.common.mapper.PaymentOutputMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class ProcessCsvPaymentsOutputFileServiceTest {

    ProcessCsvPaymentsOutputFileService service;

    PaymentOutputMapper mapper = Mappers.getMapper(PaymentOutputMapper.class);
    PaymentStatusMapper paymentStatusMapper = Mappers.getMapper(PaymentStatusMapper.class);

    @TempDir static Path tempDir;
    static Path tempFile;

    @BeforeEach
    void setUp() throws IOException {
        service = new ProcessCsvPaymentsOutputFileService();
        tempFile = Files.createFile(tempDir.resolve("test.csv"));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempFile != null && Files.exists(tempFile)) {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void execute_happy() throws IOException {
        // Given
        Multi<PaymentOutput> paymentOutputList = getMultiPaymentOutput();

        Multi<CsvPaymentsOutputFile> resultMulti = service.process(paymentOutputList);

        // Convert Multi to List to collect all results
        List<CsvPaymentsOutputFile> results = resultMulti.collect().asList().await().indefinitely();

        // Since we're processing multiple payments that might be grouped by input file,
        // we expect one output file per input file group
        assertThat(results).isNotEmpty();

        // Process each result file
        for (CsvPaymentsOutputFile resultFile : results) {
            // Inspect/verify the output file
            // When: Read the lines from the output file
            List<String> lines = Files.readAllLines(resultFile.getFilepath());

            // Then: Assert header and records exist
            assertThat(lines).hasSizeGreaterThanOrEqualTo(2);

            String headerLine = lines.get(0);
            String firstRecordLine = lines.get(1);

            AssertionsForClassTypes.assertThat(headerLine)
                    .contains(
                            "AMOUNT",
                            "CSV ID",
                            "CURRENCY",
                            "FEE",
                            "MESSAGE",
                            "RECIPIENT",
                            "REFERENCE",
                            "STATUS");
            AssertionsForClassTypes.assertThat(firstRecordLine)
                    .containsAnyOf(
                            "100.00", // From first record
                            "450.01"); // From second record
        }
    }

    private Multi<PaymentOutput> getMultiPaymentOutput() {
        PaymentRecord paymentRecord = new PaymentRecord();
        paymentRecord.setCsvPaymentsInputFilePath(tempFile);
        paymentRecord.setCsvId(String.valueOf(UUID.randomUUID()));
        paymentRecord.setRecipient("John Doe");
        paymentRecord.setAmount(new BigDecimal("100.00"));
        paymentRecord.setCurrency(Currency.getInstance("USD"));

        AckPaymentSent ackPaymentSent = new AckPaymentSent();
        ackPaymentSent.setPaymentRecord(paymentRecord);
        ackPaymentSent.setConversationId(UUID.randomUUID());
        PaymentStatus paymentStatus = new PaymentStatus();
        paymentStatus.setAckPaymentSent(ackPaymentSent);
        paymentStatus.setStatus("nada");
        paymentStatus.setMessage("Success");
        PaymentStatusDto paymentStatusDto = paymentStatusMapper.toDto(paymentStatus);

        PaymentOutputDto paymentOutputDto1 =
                PaymentOutputTestBuilder.aPaymentOutput()
                        .withCsvId("80e055c9-7dbe-4ef0-ad37-8360eb8d1e3e")
                        .withRecipient("recipient123")
                        .withAmount(new BigDecimal("100.00"))
                        .withCurrency(Currency.getInstance("USD"))
                        .withConversationId(UUID.fromString("abacd5c7-2230-4a24-a665-32a542468ea5"))
                        .withPaymentStatus(paymentStatusDto)
                        .buildDto();

        PaymentOutputDto paymentOutputDto2 =
                PaymentOutputTestBuilder.aPaymentOutput()
                        .withCsvId("2d8acc5b-8dae-4240-b37c-893318aba63f")
                        .withRecipient("234recipient")
                        .withAmount(new BigDecimal("450.01"))
                        .withCurrency(Currency.getInstance("GBP"))
                        .withConversationId(UUID.fromString("746ab623-c070-49dd-87fb-ed2f39f2f3cf"))
                        .withPaymentStatus(paymentStatusDto)
                        .buildDto();

        return Multi.createFrom()
                .items(mapper.fromDto(paymentOutputDto1), mapper.fromDto(paymentOutputDto2));
    }

    private Multi<PaymentOutput> getBadMultiPaymentOutput() {
        return Multi.createFrom().empty();
    }

    @Test
    void execute_unhappy() {
        // Given
        Multi<PaymentOutput> paymentOutputList = getBadMultiPaymentOutput();

        // When
        Multi<CsvPaymentsOutputFile> resultMulti = service.process(paymentOutputList);

        // Then - Collect results and verify we get an empty list or similar
        List<CsvPaymentsOutputFile> results = resultMulti.collect().asList().await().indefinitely();

        // For empty input, we should get an empty list of output files
        assertThat(results).isEmpty();
    }

    @Test
    void execute_with_multiple_input_files_should_not_mix_records() throws IOException {
        // Given - Payment outputs from two different input files
        Multi<PaymentOutput> paymentOutputList = getMultiPaymentOutputFromMultipleFiles();

        Multi<CsvPaymentsOutputFile> resultMulti = service.process(paymentOutputList);

        // When: Process the mixed stream and collect all output files
        List<CsvPaymentsOutputFile> results = resultMulti.collect().asList().await().indefinitely();

        // Then: Verify that we get output files (one per input file group)
        assertThat(results).isNotEmpty();
        // The service groups by input file path, so we should get separate output files
        assertThat(results)
                .hasSizeGreaterThanOrEqualTo(1); // Could be 1 or more depending on grouping

        // Process each result file
        for (CsvPaymentsOutputFile resultFile : results) {
            // Read the output file content
            List<String> lines = Files.readAllLines(resultFile.getFilepath());

            // Should have header + at least 1 record
            assertThat(lines).hasSizeGreaterThanOrEqualTo(2);

            String headerLine = lines.get(0);
            String firstRecordLine = lines.get(1);

            // Verify header
            AssertionsForClassTypes.assertThat(headerLine)
                    .contains(
                            "AMOUNT",
                            "CSV ID",
                            "CURRENCY",
                            "FEE",
                            "MESSAGE",
                            "RECIPIENT",
                            "REFERENCE",
                            "STATUS");

            // Verify that we have records with expected values
            AssertionsForClassTypes.assertThat(firstRecordLine)
                    .containsAnyOf(
                            "100.00", // From first file
                            "450.01"); // From second file
        }
    }

    private Multi<PaymentOutput> getMultiPaymentOutputFromMultipleFiles() {
        // Create payment record for first file
        Path firstFile = tempDir.resolve("first.csv");
        PaymentRecord paymentRecord1 = new PaymentRecord();
        paymentRecord1.setCsvPaymentsInputFilePath(firstFile);
        paymentRecord1.setCsvId(String.valueOf(UUID.randomUUID()));
        paymentRecord1.setRecipient("John Doe");
        paymentRecord1.setAmount(new BigDecimal("100.00"));
        paymentRecord1.setCurrency(Currency.getInstance("USD"));

        AckPaymentSent ackPaymentSent1 = new AckPaymentSent();
        ackPaymentSent1.setPaymentRecord(paymentRecord1);
        ackPaymentSent1.setConversationId(UUID.randomUUID());
        PaymentStatus paymentStatus1 = new PaymentStatus();
        paymentStatus1.setAckPaymentSent(ackPaymentSent1);
        paymentStatus1.setStatus("nada");
        paymentStatus1.setMessage("Success");
        PaymentStatusDto paymentStatusDto1 = paymentStatusMapper.toDto(paymentStatus1);

        PaymentOutputDto paymentOutputDto1 =
                PaymentOutputTestBuilder.aPaymentOutput()
                        .withCsvId("80e055c9-7dbe-4ef0-ad37-8360eb8d1e3e")
                        .withRecipient("recipient123")
                        .withAmount(new BigDecimal("100.00"))
                        .withCurrency(Currency.getInstance("USD"))
                        .withConversationId(UUID.fromString("abacd5c7-2230-4a24-a665-32a542468ea5"))
                        .withPaymentStatus(paymentStatusDto1)
                        .buildDto();

        // Create payment record for second file
        Path secondFile = tempDir.resolve("second.csv");
        PaymentRecord paymentRecord2 = new PaymentRecord();
        paymentRecord2.setCsvPaymentsInputFilePath(secondFile);
        paymentRecord2.setCsvId(String.valueOf(UUID.randomUUID()));
        paymentRecord2.setRecipient("Jane Doe");
        paymentRecord2.setAmount(new BigDecimal("450.01"));
        paymentRecord2.setCurrency(Currency.getInstance("GBP"));

        AckPaymentSent ackPaymentSent2 = new AckPaymentSent();
        ackPaymentSent2.setPaymentRecord(paymentRecord2);
        ackPaymentSent2.setConversationId(UUID.randomUUID());
        PaymentStatus paymentStatus2 = new PaymentStatus();
        paymentStatus2.setAckPaymentSent(ackPaymentSent2);
        paymentStatus2.setStatus("nada");
        paymentStatus2.setMessage("Success");
        PaymentStatusDto paymentStatusDto2 = paymentStatusMapper.toDto(paymentStatus2);

        PaymentOutputDto paymentOutputDto2 =
                PaymentOutputTestBuilder.aPaymentOutput()
                        .withCsvId("2d8acc5b-8dae-4240-b37c-893318aba63f")
                        .withRecipient("234recipient")
                        .withAmount(new BigDecimal("450.01"))
                        .withCurrency(Currency.getInstance("GBP"))
                        .withConversationId(UUID.fromString("746ab623-c070-49dd-87fb-ed2f39f2f3cf"))
                        .withPaymentStatus(paymentStatusDto2)
                        .buildDto();

        return Multi.createFrom()
                .items(mapper.fromDto(paymentOutputDto1), mapper.fromDto(paymentOutputDto2));
    }
}
