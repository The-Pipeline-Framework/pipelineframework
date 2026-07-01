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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus;

class PaymentStatusVariantMapperTest {

  private ApprovedPaymentStatusMapper approvedMapper;
  private UnapprovedPaymentStatusMapper unapprovedMapper;

  @BeforeEach
  void setUp() {
    CommonConverters commonConverters = new CommonConverters();

    ApprovedPaymentStatusMapperImpl approvedImpl = new ApprovedPaymentStatusMapperImpl();
    setField(approvedImpl, "commonConverters", commonConverters);
    approvedMapper = approvedImpl;

    UnapprovedPaymentStatusMapperImpl unapprovedImpl = new UnapprovedPaymentStatusMapperImpl();
    setField(unapprovedImpl, "commonConverters", commonConverters);
    unapprovedMapper = unapprovedImpl;
  }

  @Test
  void approvedStatusRoundTripsThroughGrpcMapper() {
    ApprovedPaymentStatus status = approvedStatus();

    ApprovedPaymentStatus mapped = approvedMapper.fromExternal(approvedMapper.toExternal(status));

    assertNotNull(mapped);
    assertEquals(status.getReference(), mapped.getReference());
    assertEquals(status.getStatus(), mapped.getStatus());
    assertEquals(status.getMessage(), mapped.getMessage());
    assertEquals(status.getFee(), mapped.getFee());
    assertEquals(status.getConversationId(), mapped.getConversationId());
    assertEquals(status.getStatusCode(), mapped.getStatusCode());
    assertEquals(status.getPaymentRecordId(), mapped.getPaymentRecordId());
    assertNotNull(mapped.getPaymentRecord());
    assertEquals(
        status.getPaymentRecord().getCsvPaymentsInputFilePath(),
        mapped.getPaymentRecord().getCsvPaymentsInputFilePath());
  }

  @Test
  void unapprovedStatusRoundTripsThroughGrpcMapper() {
    UnapprovedPaymentStatus status = unapprovedStatus();

    UnapprovedPaymentStatus mapped =
        unapprovedMapper.fromExternal(unapprovedMapper.toExternal(status));

    assertNotNull(mapped);
    assertEquals(status.getReference(), mapped.getReference());
    assertEquals(status.getStatus(), mapped.getStatus());
    assertEquals(status.getMessage(), mapped.getMessage());
    assertEquals(status.getFee(), mapped.getFee());
    assertEquals(status.getConversationId(), mapped.getConversationId());
    assertEquals(status.getStatusCode(), mapped.getStatusCode());
    assertEquals(status.getPaymentRecordId(), mapped.getPaymentRecordId());
    assertNotNull(mapped.getPaymentRecord());
    assertEquals(
        status.getPaymentRecord().getCsvPaymentsInputFilePath(),
        mapped.getPaymentRecord().getCsvPaymentsInputFilePath());
  }


  private static ApprovedPaymentStatus approvedStatus() {
    ApprovedPaymentStatus status = new ApprovedPaymentStatus();
    status.setId(UUID.randomUUID());
    status.setReference("approved-ref");
    status.setStatus("Complete");
    status.setMessage("settled");
    status.setFee(new BigDecimal("1.01"));
    status.setConversationId(UUID.randomUUID());
    status.setStatusCode(1000L);
    status.setPaymentRecord(paymentRecord());
    status.setPaymentRecordId(status.getPaymentRecord().getId());
    return status;
  }

  private static UnapprovedPaymentStatus unapprovedStatus() {
    UnapprovedPaymentStatus status = new UnapprovedPaymentStatus();
    status.setId(UUID.randomUUID());
    status.setReference("rejected-ref");
    status.setStatus("Rejected");
    status.setMessage("declined");
    status.setFee(new BigDecimal("1.01"));
    status.setConversationId(UUID.randomUUID());
    status.setStatusCode(400L);
    status.setPaymentRecord(paymentRecord());
    status.setPaymentRecordId(status.getPaymentRecord().getId());
    return status;
  }

  private static PaymentRecord paymentRecord() {
    PaymentRecord paymentRecord = new PaymentRecord();
    paymentRecord.setId(UUID.randomUUID());
    paymentRecord.setCsvId("csv-1");
    paymentRecord.setRecipient("Alice");
    paymentRecord.setAmount(new BigDecimal("12.34"));
    paymentRecord.setCurrency(Currency.getInstance("EUR"));
    paymentRecord.setCsvPaymentsInputFilePath(Path.of("/tmp/payments.csv"));
    return paymentRecord;
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
