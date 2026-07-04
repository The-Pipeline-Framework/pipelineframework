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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.smallrye.mutiny.Multi;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.csv.common.domain.CsvPaymentsOutputFile;
import org.pipelineframework.csv.common.domain.PaymentOutput;

class ProcessCsvPaymentsOutputFileServiceTest {

  ProcessCsvPaymentsOutputFileService service;

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
    List<CsvPaymentsOutputFile> results =
        service.process(multiPaymentOutput()).collect().asList().await().indefinitely();

    assertThat(results).isNotEmpty();
    for (CsvPaymentsOutputFile resultFile : results) {
      List<String> lines = Files.readAllLines(resultFile.getFilepath());
      assertThat(lines).hasSizeGreaterThanOrEqualTo(2);
      assertThat(lines.get(0))
          .contains("AMOUNT", "CSV ID", "CURRENCY", "FEE", "MESSAGE", "RECIPIENT", "REFERENCE", "STATUS");
      assertThat(lines.get(1)).containsAnyOf("100.00", "450.01");
    }
  }

  @Test
  void execute_unhappy() {
    List<CsvPaymentsOutputFile> results =
        service.process(Multi.createFrom().empty()).collect().asList().await().indefinitely();

    assertThat(results).isEmpty();
  }

  @Test
  void execute_with_multiple_input_files_should_not_mix_records() throws IOException {
    List<CsvPaymentsOutputFile> results =
        service.process(multiPaymentOutputFromMultipleFiles()).collect().asList().await().indefinitely();

    assertThat(results).isNotEmpty();
    for (CsvPaymentsOutputFile resultFile : results) {
      List<String> lines = Files.readAllLines(resultFile.getFilepath());
      assertThat(lines).hasSizeGreaterThanOrEqualTo(2);
      assertThat(lines.get(0))
          .contains("AMOUNT", "CSV ID", "CURRENCY", "FEE", "MESSAGE", "RECIPIENT", "REFERENCE", "STATUS");
      assertThat(lines.get(1)).containsAnyOf("100.00", "450.01");
    }
  }

  private Multi<PaymentOutput> multiPaymentOutput() {
    PaymentOutput first =
        paymentOutput(
            tempFile,
            "80e055c9-7dbe-4ef0-ad37-8360eb8d1e3e",
            "recipient123",
            new BigDecimal("100.00"),
            Currency.getInstance("USD"),
            UUID.fromString("abacd5c7-2230-4a24-a665-32a542468ea5"));
    PaymentOutput second =
        paymentOutput(
            tempFile,
            "2d8acc5b-8dae-4240-b37c-893318aba63f",
            "234recipient",
            new BigDecimal("450.01"),
            Currency.getInstance("GBP"),
            UUID.fromString("746ab623-c070-49dd-87fb-ed2f39f2f3cf"));
    return Multi.createFrom().items(first, second);
  }

  private Multi<PaymentOutput> multiPaymentOutputFromMultipleFiles() {
    Path firstFile = tempDir.resolve("first.csv");
    Path secondFile = tempDir.resolve("second.csv");
    PaymentOutput first =
        paymentOutput(
            firstFile,
            "80e055c9-7dbe-4ef0-ad37-8360eb8d1e3e",
            "recipient123",
            new BigDecimal("100.00"),
            Currency.getInstance("USD"),
            UUID.fromString("abacd5c7-2230-4a24-a665-32a542468ea5"));
    PaymentOutput second =
        paymentOutput(
            secondFile,
            "2d8acc5b-8dae-4240-b37c-893318aba63f",
            "234recipient",
            new BigDecimal("450.01"),
            Currency.getInstance("GBP"),
            UUID.fromString("746ab623-c070-49dd-87fb-ed2f39f2f3cf"));
    return Multi.createFrom().items(first, second);
  }

  private PaymentOutput paymentOutput(
      Path inputFile,
      String csvId,
      String recipient,
      BigDecimal amount,
      Currency currency,
      UUID conversationId) {
    PaymentOutput output = new PaymentOutput();
    output.setCsvPaymentsInputFilePath(inputFile);
    output.setCsvPaymentsOutputFilename(inputFile.getFileName().toString());
    output.setCsvId(csvId);
    output.setRecipient(recipient);
    output.setAmount(amount);
    output.setCurrency(currency);
    output.setConversationId(conversationId);
    output.setStatus(0L);
    output.setMessage("");
    output.setFee(null);
    return output;
  }
}
