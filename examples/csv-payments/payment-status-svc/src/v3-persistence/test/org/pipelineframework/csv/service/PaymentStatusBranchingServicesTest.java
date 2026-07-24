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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.domain.ApprovedPaymentOutput;
import org.pipelineframework.csv.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.domain.PaymentOutput;
import org.pipelineframework.csv.domain.PaymentOutputBranch;
import org.pipelineframework.csv.domain.PaymentRecord;
import org.pipelineframework.csv.domain.UnapprovedPaymentOutput;
import org.pipelineframework.csv.domain.UnapprovedPaymentStatus;
import org.pipelineframework.step.NonRetryableException;

class PaymentStatusBranchingServicesTest {

  private final ProcessApprovedPaymentStatusService approvedService =
      new ProcessApprovedPaymentStatusService();
  private final ProcessUnapprovedPaymentStatusService unapprovedService =
      new ProcessUnapprovedPaymentStatusService();
  private final ProcessFinalizePaymentOutputService finalizeService =
      new ProcessFinalizePaymentOutputService();

  @Test
  void approvedStatusMapsToApprovedBranchOutput() {
    ApprovedPaymentStatus status = approvedStatus();

    ApprovedPaymentOutput output = approvedService.process(status).await().indefinitely();

    assertEquals("payments.csv", output.csvPaymentsOutputFilename());
    assertEquals(Path.of("/tmp/payments.csv"), output.csvPaymentsInputFilePath());
    assertEquals("csv-1", output.csvId());
    assertEquals("alice", output.recipient());
    assertEquals(202L, output.status());
    assertEquals("settled", output.message());
  }

  @Test
  void unapprovedStatusMapsToUnapprovedBranchOutput() {
    UnapprovedPaymentStatus status = unapprovedStatus();

    UnapprovedPaymentOutput output = unapprovedService.process(status).await().indefinitely();

    assertEquals("payments.csv", output.csvPaymentsOutputFilename());
    assertEquals(Path.of("/tmp/payments.csv"), output.csvPaymentsInputFilePath());
    assertEquals("csv-1", output.csvId());
    assertEquals("alice", output.recipient());
    assertEquals(400L, output.status());
    assertEquals("declined", output.message());
  }

  @Test
  void finalMergeCopiesBranchOutputIntoPublishedPaymentOutput() {
    ApprovedPaymentOutput branchOutput =
        approvedService.process(approvedStatus()).await().indefinitely();

    PaymentOutput finalOutput = finalizeService.process(new PaymentOutputBranch.Approved(branchOutput)).await().indefinitely();

    assertEquals(branchOutput.csvPaymentsOutputFilename(), finalOutput.csvPaymentsOutputFilename());
    assertEquals(branchOutput.csvPaymentsInputFilePath(), finalOutput.csvPaymentsInputFilePath());
    assertEquals(branchOutput.csvId(), finalOutput.csvId());
    assertEquals(branchOutput.recipient(), finalOutput.recipient());
    assertEquals(branchOutput.amount(), finalOutput.amount());
    assertEquals(branchOutput.currency(), finalOutput.currency());
    assertEquals(branchOutput.conversationId(), finalOutput.conversationId());
    assertEquals(branchOutput.status(), finalOutput.status());
    assertEquals(branchOutput.message(), finalOutput.message());
    assertEquals(branchOutput.fee(), finalOutput.fee());
  }

  @Test
  void finalMergeCopiesUnapprovedBranchOutputIntoPublishedPaymentOutput() {
    UnapprovedPaymentOutput branchOutput =
        unapprovedService.process(unapprovedStatus()).await().indefinitely();

    PaymentOutput finalOutput = finalizeService.process(new PaymentOutputBranch.Unapproved(branchOutput)).await().indefinitely();

    assertEquals(branchOutput.csvPaymentsOutputFilename(), finalOutput.csvPaymentsOutputFilename());
    assertEquals(branchOutput.csvPaymentsInputFilePath(), finalOutput.csvPaymentsInputFilePath());
    assertEquals(branchOutput.csvId(), finalOutput.csvId());
    assertEquals(branchOutput.recipient(), finalOutput.recipient());
    assertEquals(branchOutput.amount(), finalOutput.amount());
    assertEquals(branchOutput.currency(), finalOutput.currency());
    assertEquals(branchOutput.conversationId(), finalOutput.conversationId());
    assertEquals(branchOutput.status(), finalOutput.status());
    assertEquals(branchOutput.message(), finalOutput.message());
    assertEquals(branchOutput.fee(), finalOutput.fee());
  }

  @Test
  void approvedStatusWithoutPaymentRecordFailsFast() {
    ApprovedPaymentStatus status = new ApprovedPaymentStatus("provider-ref", "Complete", "settled", new BigDecimal("0.12"), UUID.randomUUID(), 202L, null, null);

    NonRetryableException failure =
        assertThrows(
            NonRetryableException.class,
            () -> approvedService.process(status).await().indefinitely());

    assertEquals(
        "ApprovedPaymentStatus must include paymentRecord and csvPaymentsInputFilePath",
        failure.getMessage());
  }

  @Test
  void unapprovedStatusWithoutInputFilePathFailsFast() {
    PaymentRecord record = new PaymentRecord(UUID.randomUUID(), "csv-1", "alice", new BigDecimal("12.34"), Currency.getInstance("EUR"), null);
    UnapprovedPaymentStatus status = new UnapprovedPaymentStatus("provider-ref", "Rejected", "declined", BigDecimal.ZERO, UUID.randomUUID(), 400L, record.id(), record);

    NonRetryableException failure =
        assertThrows(
            NonRetryableException.class,
            () -> unapprovedService.process(status).await().indefinitely());

    assertEquals(
        "UnapprovedPaymentStatus must include paymentRecord and csvPaymentsInputFilePath",
        failure.getMessage());
  }

  private static PaymentRecord paymentRecord() {
    return new PaymentRecord(UUID.randomUUID(), "csv-1", "alice", new BigDecimal("12.34"),
        Currency.getInstance("EUR"), Path.of("/tmp/payments.csv"));
  }

  private static ApprovedPaymentStatus approvedStatus() {
    PaymentRecord record = paymentRecord();
    return new ApprovedPaymentStatus("provider-ref", "Complete", "settled", new BigDecimal("0.12"),
        UUID.randomUUID(), 202L, record.id(), record);
  }

  private static UnapprovedPaymentStatus unapprovedStatus() {
    PaymentRecord record = paymentRecord();
    return new UnapprovedPaymentStatus("provider-ref", "Rejected", "declined", BigDecimal.ZERO,
        UUID.randomUUID(), 400L, record.id(), record);
  }
}
