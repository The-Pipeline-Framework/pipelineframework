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

package org.pipelineframework.csv.common.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.ApprovedPaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentOutputBranch;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentOutput;

class PaymentOutputBranchMapperTest {

  private PaymentOutputBranchMapper mapper;

  @BeforeEach
  void setUp() {
    CommonConverters commonConverters = new CommonConverters();

    ApprovedPaymentOutputMapperImpl approvedMapper = new ApprovedPaymentOutputMapperImpl();
    setField(approvedMapper, "commonConverters", commonConverters);

    UnapprovedPaymentOutputMapperImpl unapprovedMapper = new UnapprovedPaymentOutputMapperImpl();
    setField(unapprovedMapper, "commonConverters", commonConverters);

    mapper = new PaymentOutputBranchMapper();
    mapper.approvedMapper = approvedMapper;
    mapper.unapprovedMapper = unapprovedMapper;
  }

  @Test
  void roundTripsApprovedBranch() {
    ApprovedPaymentOutput output = approvedOutput();

    PaymentOutputBranch mapped = mapper.fromExternal(mapper.toExternal(output));

    assertNotNull(mapped);
    ApprovedPaymentOutput approved = assertInstanceOf(ApprovedPaymentOutput.class, mapped);
    assertEquals(output.getCsvId(), approved.getCsvId());
    assertEquals(output.getRecipient(), approved.getRecipient());
    assertEquals(output.getAmount(), approved.getAmount());
    assertEquals(output.getCurrency(), approved.getCurrency());
    assertEquals(output.getConversationId(), approved.getConversationId());
    assertEquals(output.getStatus(), approved.getStatus());
    assertEquals(output.getMessage(), approved.getMessage());
    assertEquals(output.getFee(), approved.getFee());
  }

  @Test
  void roundTripsUnapprovedBranch() {
    UnapprovedPaymentOutput output = unapprovedOutput();

    PaymentOutputBranch mapped = mapper.fromExternal(mapper.toExternal(output));

    assertNotNull(mapped);
    UnapprovedPaymentOutput unapproved = assertInstanceOf(UnapprovedPaymentOutput.class, mapped);
    assertEquals(output.getCsvId(), unapproved.getCsvId());
    assertEquals(output.getRecipient(), unapproved.getRecipient());
    assertEquals(output.getAmount(), unapproved.getAmount());
    assertEquals(output.getCurrency(), unapproved.getCurrency());
    assertEquals(output.getConversationId(), unapproved.getConversationId());
    assertEquals(output.getStatus(), unapproved.getStatus());
    assertEquals(output.getMessage(), unapproved.getMessage());
    assertEquals(output.getFee(), unapproved.getFee());
  }

  private static ApprovedPaymentOutput approvedOutput() {
    ApprovedPaymentOutput output = new ApprovedPaymentOutput();
    populate(output, 200L, "approved");
    return output;
  }

  private static UnapprovedPaymentOutput unapprovedOutput() {
    UnapprovedPaymentOutput output = new UnapprovedPaymentOutput();
    populate(output, 400L, "rejected");
    return output;
  }

  private static void populate(PaymentOutputBranch output, long status, String message) {
    output.setId(UUID.randomUUID());
    output.setCsvPaymentsOutputFilename("payments.csv");
    output.setCsvPaymentsInputFilePath(Path.of("/tmp/payments.csv"));
    output.setCsvId("csv-1");
    output.setRecipient("Alice");
    output.setAmount(new BigDecimal("12.34"));
    output.setCurrency(Currency.getInstance("EUR"));
    output.setConversationId(UUID.randomUUID());
    output.setStatus(status);
    output.setMessage(message);
    output.setFee(new BigDecimal("1.01"));
  }

  private static void setField(Object target, String fieldName, Object value) {
    try {
      java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to set mapper dependency " + fieldName, e);
    }
  }
}
