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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.dto.PaymentStatusDto;
import org.pipelineframework.csv.grpc.PipelineTypes;

class PaymentStatusMapperTest {

  private PaymentStatusMapper mapper;
  private PaymentRecordMapper paymentRecordMapper;

  @BeforeEach
  void setUp() {
    CommonConverters commonConverters = new CommonConverters();
    PaymentRecordMapperImpl paymentRecordMapperImpl = new PaymentRecordMapperImpl();
    setField(paymentRecordMapperImpl, "commonConverters", commonConverters);
    paymentRecordMapper = paymentRecordMapperImpl;

    PaymentStatusMapperImpl paymentStatusMapperImpl = new PaymentStatusMapperImpl();
    setField(paymentStatusMapperImpl, "commonConverters", commonConverters);
    setField(paymentStatusMapperImpl, "paymentRecordMapper", paymentRecordMapper);
    mapper = paymentStatusMapperImpl;
  }

  @Test
  void testDomainToDto() {
    PaymentRecord paymentRecord = createTestPaymentRecord(UUID.randomUUID());
    PaymentStatus domain = createTestPaymentStatus(paymentRecord);

    PaymentStatusDto dto = mapper.toDto(domain);

    assertNotNull(dto);
    assertEquals(domain.getId(), dto.getId());
    assertEquals(domain.getReference(), dto.getReference());
    assertEquals(domain.getStatus(), dto.getStatus());
    assertEquals(domain.getMessage(), dto.getMessage());
    assertEquals(domain.getFee(), dto.getFee());
    assertEquals(domain.getConversationId(), dto.getConversationId());
    assertEquals(domain.getStatusCode(), dto.getStatusCode());
    assertEquals(domain.getPaymentRecordId(), dto.getPaymentRecordId());
    assertEquals(paymentRecordMapper.toDto(paymentRecord), dto.getPaymentRecord());
  }

  @Test
  void testDtoToDomain() {
    PaymentRecord paymentRecord = createTestPaymentRecord(UUID.randomUUID());
    PaymentStatusDto dto = PaymentStatusDto.builder()
        .id(UUID.randomUUID())
        .reference("test-ref")
        .status("SUCCESS")
        .message("Payment processed successfully")
        .fee(new BigDecimal("1.50"))
        .conversationId(UUID.randomUUID())
        .statusCode(1000L)
        .paymentRecordId(paymentRecord.getId())
        .paymentRecord(paymentRecordMapper.toDto(paymentRecord))
        .build();

    PaymentStatus domain = mapper.fromDto(dto);

    assertNotNull(domain);
    assertEquals(dto.getId(), domain.getId());
    assertEquals(dto.getReference(), domain.getReference());
    assertEquals(dto.getStatus(), domain.getStatus());
    assertEquals(dto.getMessage(), domain.getMessage());
    assertEquals(dto.getFee(), domain.getFee());
    assertEquals(dto.getConversationId(), domain.getConversationId());
    assertEquals(dto.getStatusCode(), domain.getStatusCode());
    assertEquals(dto.getPaymentRecordId(), domain.getPaymentRecordId());
    assertEquals(dto.getPaymentRecord(), paymentRecordMapper.toDto(domain.getPaymentRecord()));
  }

  @Test
  void testGrpcToDto() {
    UUID id = UUID.randomUUID();
    UUID conversationId = UUID.randomUUID();
    UUID paymentRecordId = UUID.randomUUID();

    PipelineTypes.PaymentStatus grpc = PipelineTypes.PaymentStatus.newBuilder()
        .setId(id.toString())
        .setReference("test-ref")
        .setStatus("SUCCESS")
        .setMessage("Payment processed successfully")
        .setFee("1.50")
        .setConversationId(conversationId.toString())
        .setStatusCode(1000L)
        .setPaymentRecordId(paymentRecordId.toString())
        .build();

    PaymentStatusDto dto = mapper.fromGrpc(grpc);

    assertNotNull(dto);
    assertEquals(id, dto.getId());
    assertEquals("test-ref", dto.getReference());
    assertEquals("SUCCESS", dto.getStatus());
    assertEquals("Payment processed successfully", dto.getMessage());
    assertEquals(new BigDecimal("1.50"), dto.getFee());
    assertEquals(conversationId, dto.getConversationId());
    assertEquals(1000L, dto.getStatusCode());
    assertEquals(paymentRecordId, dto.getPaymentRecordId());
    assertNull(dto.getPaymentRecord());
  }

  @Test
  void testGrpcToDomain() {
    UUID id = UUID.randomUUID();
    UUID conversationId = UUID.randomUUID();
    UUID paymentRecordId = UUID.randomUUID();

    PipelineTypes.PaymentStatus grpc = PipelineTypes.PaymentStatus.newBuilder()
        .setId(id.toString())
        .setReference("test-ref")
        .setStatus("SUCCESS")
        .setMessage("Payment processed successfully")
        .setFee("1.50")
        .setConversationId(conversationId.toString())
        .setStatusCode(1000L)
        .setPaymentRecordId(paymentRecordId.toString())
        .build();

    PaymentStatus domain = mapper.fromGrpcFromDto(grpc);

    assertNotNull(domain);
    assertEquals(id, domain.getId());
    assertEquals("test-ref", domain.getReference());
    assertEquals("SUCCESS", domain.getStatus());
    assertEquals("Payment processed successfully", domain.getMessage());
    assertEquals(new BigDecimal("1.50"), domain.getFee());
    assertEquals(conversationId, domain.getConversationId());
    assertEquals(1000L, domain.getStatusCode());
    assertEquals(paymentRecordId, domain.getPaymentRecordId());
    assertNull(domain.getPaymentRecord());
  }

  private static PaymentStatus createTestPaymentStatus(PaymentRecord paymentRecord) {
    PaymentStatus paymentStatus = new PaymentStatus();
    paymentStatus.setId(UUID.randomUUID());
    paymentStatus.setReference("test-ref");
    paymentStatus.setStatus("SUCCESS");
    paymentStatus.setMessage("Payment processed successfully");
    paymentStatus.setFee(new BigDecimal("1.50"));
    paymentStatus.setConversationId(UUID.randomUUID());
    paymentStatus.setStatusCode(1000L);
    paymentStatus.setPaymentRecordId(paymentRecord.getId());
    paymentStatus.setPaymentRecord(paymentRecord);
    return paymentStatus;
  }

  private static PaymentRecord createTestPaymentRecord(UUID id) {
    PaymentRecord paymentRecord = new PaymentRecord();
    paymentRecord.setId(id);
    paymentRecord.setCsvId("test-record");
    paymentRecord.setRecipient("Test Recipient");
    paymentRecord.setAmount(new BigDecimal("100.50"));
    paymentRecord.setCurrency(Currency.getInstance("EUR"));
    paymentRecord.setCsvPaymentsInputFilePath(Path.of("/test/path/file.csv"));
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
