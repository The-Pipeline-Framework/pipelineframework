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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.dto.AckPaymentSentDto;
import org.pipelineframework.csv.common.dto.PaymentRecordDto;
import org.pipelineframework.csv.grpc.ProcessSendPaymentRecordSvc;

import static org.junit.jupiter.api.Assertions.*;

class AckPaymentSentMapperTest {

    private AckPaymentSentMapper mapper;
    private CommonConverters commonConverters;
    private PaymentRecordMapper paymentRecordMapper;

    @BeforeEach
    void setUp() {
        commonConverters = new CommonConverters();

        // Create PaymentRecordMapperImpl and set its dependencies
        PaymentRecordMapperImpl paymentRecordMapperImpl = new PaymentRecordMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    PaymentRecordMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(paymentRecordMapperImpl, commonConverters);
            paymentRecordMapper = paymentRecordMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set PaymentRecordMapper dependencies", e);
        }

        // Create AckPaymentSentMapperImpl and set its dependencies
        AckPaymentSentMapperImpl ackPaymentSentMapperImpl = new AckPaymentSentMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    AckPaymentSentMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(ackPaymentSentMapperImpl, commonConverters);

            java.lang.reflect.Field paymentRecordMapperField =
                    AckPaymentSentMapperImpl.class.getDeclaredField("paymentRecordMapper");
            paymentRecordMapperField.setAccessible(true);
            paymentRecordMapperField.set(ackPaymentSentMapperImpl, paymentRecordMapper);

            mapper = ackPaymentSentMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set AckPaymentSentMapper dependencies", e);
        }
    }

    // Create a nested entity if required
    private PaymentRecord createTestPaymentRecord() {
        PaymentRecord paymentRecord = new PaymentRecord();
        paymentRecord.setId(UUID.randomUUID());
        paymentRecord.setCsvId("test-record");
        paymentRecord.setRecipient("Test Recipient");
        paymentRecord.setAmount(new BigDecimal("100.50"));
        paymentRecord.setCurrency(Currency.getInstance("EUR"));
        paymentRecord.setCsvPaymentsInputFilePath(Path.of("/test/path/file.csv"));
        return paymentRecord;
    }

    @Test
    void testDomainToDto() {
        // Given
        AckPaymentSent domain = new AckPaymentSent(UUID.randomUUID());
        domain.setId(UUID.randomUUID());
        domain.setStatus(200L);
        domain.setMessage("Success");
        domain.setPaymentRecordId(UUID.randomUUID());

        PaymentRecord paymentRecord = createTestPaymentRecord();
        PaymentRecordDto paymentRecordDto = paymentRecordMapper.toDto(paymentRecord);
        domain.setPaymentRecord(paymentRecord);

        // When
        AckPaymentSentDto dto = mapper.toDto(domain);

        // Then
        assertNotNull(dto);
        assertEquals(domain.getId(), dto.getId());
        assertEquals(domain.getConversationId(), dto.getConversationId());
        assertEquals(domain.getStatus(), dto.getStatus());
        assertEquals(domain.getMessage(), dto.getMessage());
        assertEquals(domain.getPaymentRecordId(), dto.getPaymentRecordId());
        PaymentRecordDto expectedPaymentRecord = paymentRecordMapper.toDto(paymentRecord);
        assertEquals(expectedPaymentRecord, dto.getPaymentRecord());
    }

    @Test
    void testDtoToDomain() {
        // Given
        PaymentRecord paymentRecord = createTestPaymentRecord();
        PaymentRecordDto paymentRecordDto = paymentRecordMapper.toDto(paymentRecord);

        AckPaymentSentDto dto =
                AckPaymentSentDto.builder()
                        .id(UUID.randomUUID())
                        .conversationId(UUID.randomUUID())
                        .status(200L)
                        .message("Success")
                        .paymentRecordId(UUID.randomUUID())
                        .paymentRecord(paymentRecordDto)
                        .build();

        // When
        AckPaymentSent domain = mapper.fromDto(dto);

        // Then
        assertNotNull(domain);
        assertEquals(dto.getId(), domain.getId());
        assertEquals(dto.getConversationId(), domain.getConversationId());
        assertEquals(dto.getStatus(), domain.getStatus());
        assertEquals(dto.getMessage(), domain.getMessage());
        assertEquals(dto.getPaymentRecordId(), domain.getPaymentRecordId());
        PaymentRecordDto expectedPaymentRecord = paymentRecordMapper.toDto(domain.getPaymentRecord());
        assertEquals(dto.getPaymentRecord(), expectedPaymentRecord);
    }

    @Test
    void testDtoToGrpc() {
        // Given
        PaymentRecord paymentRecord = createTestPaymentRecord();
        PaymentRecordDto paymentRecordDto = paymentRecordMapper.toDto(paymentRecord);

        AckPaymentSentDto dto =
                AckPaymentSentDto.builder()
                        .id(UUID.randomUUID())
                        .conversationId(UUID.randomUUID())
                        .status(200L)
                        .message("Success")
                        .paymentRecordId(UUID.randomUUID())
                        .paymentRecord(paymentRecordDto)
                        .build();

        // When
        ProcessSendPaymentRecordSvc.AckPaymentSent grpc = mapper.toGrpc(dto);

        // Then
        assertNotNull(grpc);
        assertEquals(dto.getId().toString(), grpc.getId());
        assertEquals(dto.getConversationId().toString(), grpc.getConversationId());
        assertEquals(dto.getStatus(), grpc.getStatus());
        assertEquals(dto.getMessage(), grpc.getMessage());
        assertEquals(dto.getPaymentRecordId().toString(), grpc.getPaymentRecordId());
    }

    @Test
    void testGrpcToDto() {
        // Given
        UUID id = UUID.randomUUID();
        UUID conversationId = UUID.randomUUID();
        UUID paymentRecordId = UUID.randomUUID();

        ProcessSendPaymentRecordSvc.AckPaymentSent grpc =
                ProcessSendPaymentRecordSvc.AckPaymentSent.newBuilder()
                        .setId(id.toString())
                        .setConversationId(conversationId.toString())
                        .setStatus(200L)
                        .setMessage("Success")
                        .setPaymentRecordId(paymentRecordId.toString())
                        .build();

        // When
        AckPaymentSentDto dto = mapper.fromGrpc(grpc);

        // Then
        assertNotNull(dto);
        assertEquals(id, dto.getId());
        assertEquals(conversationId, dto.getConversationId());
        assertEquals(200L, dto.getStatus());
        assertEquals("Success", dto.getMessage());
        assertEquals(paymentRecordId, dto.getPaymentRecordId());
        assertNull(dto.getPaymentRecord());
    }

    @Test
    void testDomainToGrpc() {
        // Given
        AckPaymentSent domain = new AckPaymentSent(UUID.randomUUID());
        domain.setId(UUID.randomUUID());
        domain.setStatus(200L);
        domain.setMessage("Success");
        domain.setPaymentRecordId(UUID.randomUUID());

        PaymentRecord paymentRecord = createTestPaymentRecord();
        PaymentRecordDto paymentRecordDto = paymentRecordMapper.toDto(paymentRecord);
        domain.setPaymentRecord(paymentRecord);

        // When
        ProcessSendPaymentRecordSvc.AckPaymentSent grpc = mapper.toDtoToGrpc(domain);

        // Then
        assertNotNull(grpc);
        assertEquals(domain.getId().toString(), grpc.getId());
        assertEquals(domain.getConversationId().toString(), grpc.getConversationId());
        assertEquals(domain.getStatus(), grpc.getStatus());
        assertEquals(domain.getMessage(), grpc.getMessage());
        assertEquals(domain.getPaymentRecordId().toString(), grpc.getPaymentRecordId());
    }

    @Test
    void testGrpcToDomain() {
        // Given
        UUID id = UUID.randomUUID();
        UUID conversationId = UUID.randomUUID();
        UUID paymentRecordId = UUID.randomUUID();

        ProcessSendPaymentRecordSvc.AckPaymentSent grpc =
                ProcessSendPaymentRecordSvc.AckPaymentSent.newBuilder()
                        .setId(id.toString())
                        .setConversationId(conversationId.toString())
                        .setStatus(200L)
                        .setMessage("Success")
                        .setPaymentRecordId(paymentRecordId.toString())
                        .build();

        // When
        AckPaymentSent domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain);
        assertEquals(id, domain.getId());
        assertEquals(conversationId, domain.getConversationId());
        assertEquals(200L, domain.getStatus());
        assertEquals("Success", domain.getMessage());
        assertEquals(paymentRecordId, domain.getPaymentRecordId());
        assertNull(domain.getPaymentRecord());
    }

    @Test
    void testSerializeDeserialize() throws Exception {
        // Build DTO
        UUID id = UUID.randomUUID();
        UUID convId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        PaymentRecord paymentRecord = createTestPaymentRecord();
        PaymentRecordDto paymentRecordDto = paymentRecordMapper.toDto(paymentRecord);

        AckPaymentSentDto dto =
                AckPaymentSentDto.builder()
                        .id(id)
                        .conversationId(convId)
                        .paymentRecordId(paymentId)
                        .paymentRecord(paymentRecordDto)
                        .message("Test message")
                        .status(200L)
                        .build();

        ObjectMapper mapper = new ObjectMapper();

        // Serialize to JSON
        String json = mapper.writeValueAsString(dto);

        // Deserialize back
        AckPaymentSentDto deserialized = mapper.readValue(json, AckPaymentSentDto.class);

        // Assert equality
        assertEquals(dto, deserialized);
    }
}
