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

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.dto.AckPaymentSentDto;
import org.pipelineframework.csv.common.dto.PaymentStatusDto;
import org.pipelineframework.csv.grpc.ProcessAckPaymentSentSvc;

import static org.junit.jupiter.api.Assertions.*;

class PaymentStatusMapperTest {

    private PaymentStatusMapper mapper;
    private CommonConverters commonConverters;
    private AckPaymentSentMapper ackPaymentSentMapper;

    @BeforeEach
    void setUp() {
        commonConverters = new CommonConverters();

        // Create AckPaymentSentMapperImpl and set its dependencies
        AckPaymentSentMapperImpl ackPaymentSentMapperImpl = new AckPaymentSentMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    AckPaymentSentMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(ackPaymentSentMapperImpl, commonConverters);

            PaymentRecordMapperImpl paymentRecordMapperImpl = new PaymentRecordMapperImpl();
            java.lang.reflect.Field paymentRecordConvertersField =
                    PaymentRecordMapperImpl.class.getDeclaredField("commonConverters");
            paymentRecordConvertersField.setAccessible(true);
            paymentRecordConvertersField.set(paymentRecordMapperImpl, commonConverters);

            java.lang.reflect.Field paymentRecordMapperField =
                    AckPaymentSentMapperImpl.class.getDeclaredField("paymentRecordMapper");
            paymentRecordMapperField.setAccessible(true);
            paymentRecordMapperField.set(ackPaymentSentMapperImpl, paymentRecordMapperImpl);

            ackPaymentSentMapper = ackPaymentSentMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set AckPaymentSentMapper dependencies", e);
        }

        // Create PaymentStatusMapperImpl and set its dependencies
        PaymentStatusMapperImpl paymentStatusMapperImpl = new PaymentStatusMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    PaymentStatusMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(paymentStatusMapperImpl, commonConverters);

            java.lang.reflect.Field ackPaymentSentMapperField =
                    PaymentStatusMapperImpl.class.getDeclaredField("ackPaymentSentMapper");
            ackPaymentSentMapperField.setAccessible(true);
            ackPaymentSentMapperField.set(paymentStatusMapperImpl, ackPaymentSentMapper);

            mapper = paymentStatusMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set PaymentStatusMapper dependencies", e);
        }
    }

    // Create a nested entity if required
    private AckPaymentSent createTestAckPaymentSent() {
        AckPaymentSent ackPaymentSent = new AckPaymentSent(UUID.randomUUID());
        ackPaymentSent.setId(UUID.randomUUID());
        ackPaymentSent.setStatus(200L);
        ackPaymentSent.setMessage("Success");
        ackPaymentSent.setPaymentRecordId(UUID.randomUUID());

        PaymentRecord paymentRecord = new PaymentRecord();
        paymentRecord.setId(UUID.randomUUID());
        paymentRecord.setCsvId("test-record");
        paymentRecord.setRecipient("Test Recipient");
        paymentRecord.setAmount(new BigDecimal("100.50"));
        paymentRecord.setCurrency(Currency.getInstance("EUR"));
        paymentRecord.setCsvPaymentsInputFilePath(Path.of("/test/path/file.csv"));
        ackPaymentSent.setPaymentRecord(paymentRecord);

        return ackPaymentSent;
    }

    @Test
    void testDomainToDto() {
        // Given
        PaymentStatus domain = new PaymentStatus();
        domain.setId(UUID.randomUUID());
        domain.setReference("test-ref");
        domain.setStatus("SUCCESS");
        domain.setMessage("Payment processed successfully");
        domain.setFee(new BigDecimal("1.50"));
        domain.setAckPaymentSentId(UUID.randomUUID());

        AckPaymentSent ackPaymentSent = createTestAckPaymentSent();
        domain.setAckPaymentSent(ackPaymentSent);

        // When
        PaymentStatusDto dto = mapper.toDto(domain);

        // Then
        assertNotNull(dto);
        assertEquals(domain.getId(), dto.getId());
        assertEquals(domain.getReference(), dto.getReference());
        assertEquals(domain.getStatus(), dto.getStatus());
        assertEquals(domain.getMessage(), dto.getMessage());
        assertEquals(domain.getFee(), dto.getFee());
        assertEquals(domain.getAckPaymentSentId(), dto.getAckPaymentSentId());
        AckPaymentSentDto expectedAckPaymentSent = ackPaymentSentMapper.toDto(ackPaymentSent);
        assertEquals(expectedAckPaymentSent, dto.getAckPaymentSent());
    }

    @Test
    void testDtoToDomain() {
        // Given
        AckPaymentSent ackPaymentSent = createTestAckPaymentSent();
        AckPaymentSentDto ackPaymentSentDto = ackPaymentSentMapper.toDto(ackPaymentSent);

        PaymentStatusDto dto =
                PaymentStatusDto.builder()
                        .id(UUID.randomUUID())
                        .reference("test-ref")
                        .status("SUCCESS")
                        .message("Payment processed successfully")
                        .fee(new BigDecimal("1.50"))
                        .ackPaymentSentId(UUID.randomUUID())
                        .ackPaymentSent(ackPaymentSentDto)
                        .build();

        // When
        PaymentStatus domain = mapper.fromDto(dto);

        // Then
        assertNotNull(domain);
        assertEquals(dto.getId(), domain.getId());
        assertEquals(dto.getReference(), domain.getReference());
        assertEquals(dto.getStatus(), domain.getStatus());
        assertEquals(dto.getMessage(), domain.getMessage());
        assertEquals(dto.getFee(), domain.getFee());
        assertEquals(dto.getAckPaymentSentId(), domain.getAckPaymentSentId());
        AckPaymentSentDto expectedAckPaymentSent = ackPaymentSentMapper.toDto(domain.getAckPaymentSent());
        assertEquals(dto.getAckPaymentSent(), expectedAckPaymentSent);
    }

    // @Test
    // void testDtoToGrpc() {
    //   // Given
    //   PaymentStatusDto dto =
    //       PaymentStatusDto.builder()
    //           .id(UUID.randomUUID())
    //           .reference("test-ref")
    //           .status("SUCCESS")
    //           .message("Payment processed successfully")
    //           .fee(new BigDecimal("1.50"))
    //           .ackPaymentSentId(UUID.randomUUID())
    //           .build();

    //   // When
    //   ProcessAckPaymentSentSvc.PaymentStatus grpc = mapper.toGrpc(dto);

    //   // Then
    //   assertNotNull(grpc);
    //   assertEquals(dto.getId().toString(), grpc.getId());
    //   assertEquals(dto.getReference(), grpc.getReference());
    //   assertEquals(dto.getStatus(), grpc.getStatus());
    //   assertEquals(dto.getMessage(), grpc.getMessage());
    //   assertEquals(dto.getFee().toPlainString(), grpc.getFee());
    //   assertEquals(dto.getAckPaymentSentId().toString(), grpc.getAckPaymentSentId());
    // }

    @Test
    void testGrpcToDto() {
        // Given
        UUID id = UUID.randomUUID();
        UUID ackPaymentSentId = UUID.randomUUID();

        ProcessAckPaymentSentSvc.PaymentStatus grpc =
                ProcessAckPaymentSentSvc.PaymentStatus.newBuilder()
                        .setId(id.toString())
                        .setReference("test-ref")
                        .setStatus("SUCCESS")
                        .setMessage("Payment processed successfully")
                        .setFee("1.50")
                        .setAckPaymentSentId(ackPaymentSentId.toString())
                        .build();

        // When
        PaymentStatusDto dto = mapper.fromGrpc(grpc);

        // Then
        assertNotNull(dto);
        assertEquals(id, dto.getId());
        assertEquals("test-ref", dto.getReference());
        assertEquals("SUCCESS", dto.getStatus());
        assertEquals("Payment processed successfully", dto.getMessage());
        assertEquals(new BigDecimal("1.50"), dto.getFee());
        assertEquals(ackPaymentSentId, dto.getAckPaymentSentId());
        assertNull(dto.getAckPaymentSent());
    }

    // @Test
    // void testDomainToGrpc() {
    //   // Given
    //   PaymentStatus domain = new PaymentStatus();
    //   domain.setId(UUID.randomUUID());
    //   domain.setReference("test-ref");
    //   domain.setStatus("SUCCESS");
    //   domain.setMessage("Payment processed successfully");
    //   domain.setFee(new BigDecimal("1.50"));
    //   domain.setAckPaymentSentId(UUID.randomUUID());

    //   // When
    //   ProcessAckPaymentSentSvc.PaymentStatus grpc = mapper.toDtoToGrpc(domain);

    //   // Then
    //   assertNotNull(grpc);
    //   assertEquals(domain.getId().toString(), grpc.getId());
    //   assertEquals(domain.getReference(), grpc.getReference());
    //   assertEquals(domain.getStatus(), grpc.getStatus());
    //   assertEquals(domain.getMessage(), grpc.getMessage());
    //   assertEquals(domain.getFee().toPlainString(), grpc.getFee());
    //   assertEquals(domain.getAckPaymentSentId().toString(), grpc.getAckPaymentSentId());
    // }

    @Test
    void testGrpcToDomain() {
        // Given
        UUID id = UUID.randomUUID();
        UUID ackPaymentSentId = UUID.randomUUID();

        ProcessAckPaymentSentSvc.PaymentStatus grpc =
                ProcessAckPaymentSentSvc.PaymentStatus.newBuilder()
                        .setId(id.toString())
                        .setReference("test-ref")
                        .setStatus("SUCCESS")
                        .setMessage("Payment processed successfully")
                        .setFee("1.50")
                        .setAckPaymentSentId(ackPaymentSentId.toString())
                        .build();

        // When
        PaymentStatus domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain);
        assertEquals(id, domain.getId());
        assertEquals("test-ref", domain.getReference());
        assertEquals("SUCCESS", domain.getStatus());
        assertEquals("Payment processed successfully", domain.getMessage());
        assertEquals(new BigDecimal("1.50"), domain.getFee());
        assertEquals(ackPaymentSentId, domain.getAckPaymentSentId());
        assertNull(domain.getAckPaymentSent());
    }

    // @Test
    // void testSerializeDeserialize() throws Exception {
    //   // Build DTO
    //   PaymentStatusDto dto =
    //       PaymentStatusDto.builder()
    //           .id(UUID.randomUUID())
    //           .reference("test-ref")
    //           .status("SUCCESS")
    //           .message("Payment processed successfully")
    //           .fee(new BigDecimal("1.50"))
    //           .ackPaymentSentId(UUID.randomUUID())
    //           .build();

    //   ObjectMapper mapper = new ObjectMapper();

    //   // Serialize to JSON
    //   String json = mapper.writeValueAsString(dto);

    //   // Deserialize back
    //   PaymentStatusDto deserialized = mapper.readValue(json, PaymentStatusDto.class);

    //   // Assert equality
    //   assertEquals(dto, deserialized);
    // }
}
