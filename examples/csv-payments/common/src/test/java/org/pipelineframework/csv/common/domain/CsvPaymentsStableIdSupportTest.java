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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CsvPaymentsStableIdSupportTest {

  @Test
  void paymentRecordUsesStableBusinessIdOnPersist() {
    PaymentRecord first = paymentRecord();
    PaymentRecord duplicate = paymentRecord();
    assertNotEquals(first.getId(), duplicate.getId());

    first.useStablePaymentRecordId();
    duplicate.useStablePaymentRecordId();

    assertEquals(first.getId(), duplicate.getId());
  }

  @Test
  void paymentOutputUsesStableBusinessIdOnPersist() {
    PaymentOutput first = paymentOutput();
    PaymentOutput duplicate = paymentOutput();
    assertNotEquals(first.getId(), duplicate.getId());

    first.useStablePaymentOutputId();
    duplicate.useStablePaymentOutputId();

    assertEquals(first.getId(), duplicate.getId());
  }

  @Test
  void csvFilesUseStablePathIdOnPersist() {
    CsvPaymentsOutputFile first = new CsvPaymentsOutputFile();
    CsvPaymentsOutputFile duplicate = new CsvPaymentsOutputFile();
    first.setFilepath(Path.of("/tmp/payments.csv.out"));
    duplicate.setFilepath(Path.of("/tmp/./payments.csv.out"));
    assertNotEquals(first.getId(), duplicate.getId());

    first.useStableFileId();
    duplicate.useStableFileId();

    assertEquals(first.getId(), duplicate.getId());
  }

  @Test
  void paymentStatusUsesStableCompletionIdOnPersist() {
    UUID paymentRecordId = UUID.randomUUID();
    PaymentStatus first = paymentStatus(paymentRecordId);
    PaymentStatus duplicate = paymentStatus(paymentRecordId);
    assertNotEquals(first.getId(), duplicate.getId());

    first.useStablePaymentStatusId();
    duplicate.useStablePaymentStatusId();

    assertEquals(first.getId(), duplicate.getId());
  }

  private static PaymentRecord paymentRecord() {
    return new PaymentRecord()
        .setCsvId("row-1")
        .setRecipient("Ada Lovelace")
        .setAmount(new BigDecimal("10.00"))
        .setCurrency(Currency.getInstance("GBP"))
        .setCsvPaymentsInputFilePath(Path.of("/tmp/payments.csv"));
  }

  private static PaymentOutput paymentOutput() {
    PaymentOutput output = new PaymentOutput();
    output.setCsvId("row-1");
    output.setRecipient("Ada Lovelace");
    output.setAmount(new BigDecimal("10.00"));
    output.setCurrency(Currency.getInstance("GBP"));
    output.setConversationId(UUID.randomUUID());
    output.setStatus(1000L);
    output.setMessage("Mock response");
    output.setFee(new BigDecimal("1.01"));
    return output;
  }

  private static PaymentStatus paymentStatus(UUID paymentRecordId) {
    return new PaymentStatus()
        .setReference("101")
        .setStatus("Complete")
        .setStatusCode(1000L)
        .setMessage("Mock response")
        .setFee(new BigDecimal("1.01"))
        .setConversationId(UUID.randomUUID())
        .setPaymentRecordId(paymentRecordId);
  }
}
