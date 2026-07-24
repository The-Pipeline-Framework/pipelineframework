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
import org.pipelineframework.csv.common.domain.ApprovedPaymentOutput;
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentOutput;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus;
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

    assertEquals("payments.csv", output.getCsvPaymentsOutputFilename());
    assertEquals(Path.of("/tmp/payments.csv"), output.getCsvPaymentsInputFilePath());
    assertEquals("csv-1", output.getCsvId());
    assertEquals("alice", output.getRecipient());
    assertEquals(202L, output.getStatus());
    assertEquals("settled", output.getMessage());
  }

  @Test
  void unapprovedStatusMapsToUnapprovedBranchOutput() {
    UnapprovedPaymentStatus status = unapprovedStatus();

    UnapprovedPaymentOutput output = unapprovedService.process(status).await().indefinitely();

    assertEquals("payments.csv", output.getCsvPaymentsOutputFilename());
    assertEquals(Path.of("/tmp/payments.csv"), output.getCsvPaymentsInputFilePath());
    assertEquals("csv-1", output.getCsvId());
    assertEquals("alice", output.getRecipient());
    assertEquals(400L, output.getStatus());
    assertEquals("declined", output.getMessage());
  }

  @Test
  void finalMergeCopiesBranchOutputIntoPublishedPaymentOutput() {
    ApprovedPaymentOutput branchOutput =
        approvedService.process(approvedStatus()).await().indefinitely();

    PaymentOutput finalOutput = finalizeService.process(branchOutput).await().indefinitely();

    assertEquals(branchOutput.getCsvPaymentsOutputFilename(), finalOutput.getCsvPaymentsOutputFilename());
    assertEquals(branchOutput.getCsvPaymentsInputFilePath(), finalOutput.getCsvPaymentsInputFilePath());
    assertEquals(branchOutput.getCsvId(), finalOutput.getCsvId());
    assertEquals(branchOutput.getRecipient(), finalOutput.getRecipient());
    assertEquals(branchOutput.getAmount(), finalOutput.getAmount());
    assertEquals(branchOutput.getCurrency(), finalOutput.getCurrency());
    assertEquals(branchOutput.getConversationId(), finalOutput.getConversationId());
    assertEquals(branchOutput.getStatus(), finalOutput.getStatus());
    assertEquals(branchOutput.getMessage(), finalOutput.getMessage());
    assertEquals(branchOutput.getFee(), finalOutput.getFee());
  }

  @Test
  void finalMergeCopiesUnapprovedBranchOutputIntoPublishedPaymentOutput() {
    UnapprovedPaymentOutput branchOutput =
        unapprovedService.process(unapprovedStatus()).await().indefinitely();

    PaymentOutput finalOutput = finalizeService.process(branchOutput).await().indefinitely();

    assertEquals(branchOutput.getCsvPaymentsOutputFilename(), finalOutput.getCsvPaymentsOutputFilename());
    assertEquals(branchOutput.getCsvPaymentsInputFilePath(), finalOutput.getCsvPaymentsInputFilePath());
    assertEquals(branchOutput.getCsvId(), finalOutput.getCsvId());
    assertEquals(branchOutput.getRecipient(), finalOutput.getRecipient());
    assertEquals(branchOutput.getAmount(), finalOutput.getAmount());
    assertEquals(branchOutput.getCurrency(), finalOutput.getCurrency());
    assertEquals(branchOutput.getConversationId(), finalOutput.getConversationId());
    assertEquals(branchOutput.getStatus(), finalOutput.getStatus());
    assertEquals(branchOutput.getMessage(), finalOutput.getMessage());
    assertEquals(branchOutput.getFee(), finalOutput.getFee());
  }

  @Test
  void approvedStatusWithoutPaymentRecordFailsFast() {
    ApprovedPaymentStatus status = approvedStatus();
    status.setPaymentRecord(null);

    NonRetryableException failure =
        assertThrows(
            NonRetryableException.class,
            () -> approvedService.process(status).await().indefinitely());

    assertEquals(
        "ApprovedPaymentStatus must include paymentRecord for CSV output mapping",
        failure.getMessage());
  }

  @Test
  void unapprovedStatusWithoutInputFilePathFailsFast() {
    UnapprovedPaymentStatus status = unapprovedStatus();
    status.getPaymentRecord().setCsvPaymentsInputFilePath(null);

    NonRetryableException failure =
        assertThrows(
            NonRetryableException.class,
            () -> unapprovedService.process(status).await().indefinitely());

    assertEquals(
        "UnapprovedPaymentStatus must include csvPaymentsInputFilePath for CSV output naming",
        failure.getMessage());
  }

  private static PaymentRecord paymentRecord() {
    PaymentRecord record = new PaymentRecord();
    record.setId(UUID.randomUUID());
    record.setCsvId("csv-1");
    record.setRecipient("alice");
    record.setAmount(new BigDecimal("12.34"));
    record.setCurrency(Currency.getInstance("EUR"));
    record.setCsvPaymentsInputFilePath(Path.of("/tmp/payments.csv"));
    return record;
  }

  private static ApprovedPaymentStatus approvedStatus() {
    PaymentRecord record = paymentRecord();
    ApprovedPaymentStatus status = new ApprovedPaymentStatus();
    status.setReference("provider-ref");
    status.setStatus("Complete");
    status.setMessage("settled");
    status.setFee(new BigDecimal("0.12"));
    status.setConversationId(UUID.randomUUID());
    status.setStatusCode(202L);
    status.setPaymentRecord(record);
    status.setPaymentRecordId(record.getId());
    return status;
  }

  private static UnapprovedPaymentStatus unapprovedStatus() {
    PaymentRecord record = paymentRecord();
    UnapprovedPaymentStatus status = new UnapprovedPaymentStatus();
    status.setReference("provider-ref");
    status.setStatus("Rejected");
    status.setMessage("declined");
    status.setFee(BigDecimal.ZERO);
    status.setConversationId(UUID.randomUUID());
    status.setStatusCode(400L);
    status.setPaymentRecord(record);
    status.setPaymentRecordId(record.getId());
    return status;
  }
}
