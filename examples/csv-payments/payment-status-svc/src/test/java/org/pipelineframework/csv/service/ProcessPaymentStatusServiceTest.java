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
import java.util.Currency;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.step.NonRetryableException;

class ProcessPaymentStatusServiceTest {

  private final ProcessPaymentStatusService service = new ProcessPaymentStatusService();

  @Test
  void mapsAcceptedProviderStatusToPaymentOutput() {
    PaymentRecord record = new PaymentRecord()
        .setCsvId("csv-1")
        .setRecipient("alice")
        .setAmount(new BigDecimal("12.34"))
        .setCurrency(Currency.getInstance("EUR"));
    AckPaymentSent ack = new AckPaymentSent(UUID.randomUUID())
        .setStatus(202L)
        .setMessage("accepted")
        .setPaymentRecord(record)
        .setPaymentRecordId(record.getId());
    PaymentStatus status = new PaymentStatus()
        .setReference("provider-ref")
        .setStatus("Completed")
        .setMessage("settled")
        .setFee(new BigDecimal("0.12"))
        .setAckPaymentSent(ack)
        .setAckPaymentSentId(ack.getId())
        .setPaymentRecord(record)
        .setPaymentRecordId(record.getId());

    PaymentOutput output = service.process(status).await().indefinitely();

    assertEquals("csv-1", output.getCsvId());
    assertEquals("alice", output.getRecipient());
    assertEquals(new BigDecimal("12.34"), output.getAmount());
    assertEquals(Currency.getInstance("EUR"), output.getCurrency());
    assertEquals(ack.getConversationId(), output.getConversationId());
    assertEquals(202L, output.getStatus());
    assertEquals("settled", output.getMessage());
    assertEquals(new BigDecimal("0.12"), output.getFee());
  }

  @Test
  void rejectsTerminalProviderRejectedStatus() {
    PaymentStatus status = new PaymentStatus()
        .setReference("provider-ref")
        .setStatus("Rejected")
        .setMessage("declined")
        .setFee(BigDecimal.ZERO)
        .setAckPaymentSentId(UUID.randomUUID())
        .setPaymentRecordId(UUID.randomUUID());

    assertThrows(NonRetryableException.class, () -> service.process(status).await().indefinitely());
  }
}
