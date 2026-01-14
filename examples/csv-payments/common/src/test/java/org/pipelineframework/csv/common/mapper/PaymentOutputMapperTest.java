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
import java.util.Currency;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.dto.PaymentOutputDto;
import org.pipelineframework.csv.grpc.ProcessPaymentStatusSvc;

import static org.junit.jupiter.api.Assertions.*;

class PaymentOutputMapperTest {

    private PaymentOutputMapper mapper;
    private CommonConverters commonConverters;
    private PaymentStatusMapper paymentStatusMapper;

    @BeforeEach
    void setUp() {
        commonConverters = new CommonConverters();

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
            ackPaymentSentMapperField.set(paymentStatusMapperImpl, new AckPaymentSentMapperImpl());

            paymentStatusMapper = paymentStatusMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set PaymentStatusMapper dependencies", e);
        }

        // Create PaymentOutputMapperImpl and set its dependencies
        PaymentOutputMapperImpl paymentOutputMapperImpl = new PaymentOutputMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    PaymentOutputMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(paymentOutputMapperImpl, commonConverters);

            java.lang.reflect.Field paymentStatusMapperField =
                    PaymentOutputMapperImpl.class.getDeclaredField("paymentStatusMapper");
            paymentStatusMapperField.setAccessible(true);
            paymentStatusMapperField.set(paymentOutputMapperImpl, paymentStatusMapper);

            mapper = paymentOutputMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set PaymentOutputMapper dependencies", e);
        }
    }

    // Create a nested entity if required
    private PaymentStatus createTestPaymentStatus() {
        PaymentStatus paymentStatus = new PaymentStatus();
        paymentStatus.setId(UUID.randomUUID());
        paymentStatus.setReference("test-ref");
        paymentStatus.setStatus("SUCCESS");
        paymentStatus.setMessage("Payment processed successfully");
        paymentStatus.setFee(new BigDecimal("1.50"));
        paymentStatus.setAckPaymentSentId(UUID.randomUUID());
        return paymentStatus;
    }

    @Test
    void testDomainToDto() {
        // Given
        PaymentOutput domain = new PaymentOutput();
        UUID id = domain.getId(); // Get the ID that was automatically set by BaseEntity
        domain.setCsvId("test-record");
        domain.setRecipient("Test Recipient");
        domain.setAmount(new BigDecimal("100.50"));
        domain.setCurrency(Currency.getInstance("EUR"));
        domain.setConversationId(UUID.randomUUID());
        domain.setStatus(200L);
        domain.setMessage("Success");
        domain.setFee(new BigDecimal("1.50"));

        // When
        PaymentOutputDto dto = mapper.toDto(domain);

        // Then
        assertNotNull(dto);
        assertEquals(id, dto.getId());
        assertEquals(domain.getCsvId(), dto.getCsvId());
        assertEquals(domain.getRecipient(), dto.getRecipient());
        assertEquals(domain.getAmount(), dto.getAmount());
        assertEquals(domain.getCurrency(), dto.getCurrency());
        assertEquals(domain.getConversationId(), dto.getConversationId());
        assertEquals(domain.getStatus(), dto.getStatus());
        assertEquals(domain.getMessage(), dto.getMessage());
        assertEquals(domain.getFee(), dto.getFee());
        assertNull(dto.getPaymentStatus());
    }

    @Test
    void testDtoToDomain() {
        // Given
        UUID id = UUID.randomUUID();
        PaymentOutputDto dto =
                PaymentOutputDto.builder()
                        .id(id)
                        .csvId("test-record")
                        .recipient("Test Recipient")
                        .amount(new BigDecimal("100.50"))
                        .currency(Currency.getInstance("EUR"))
                        .conversationId(UUID.randomUUID())
                        .status(200L)
                        .message("Success")
                        .fee(new BigDecimal("1.50"))
                        .build();

        // When
        PaymentOutput domain = mapper.fromDto(dto);

        // Then
        assertNotNull(domain);
        assertEquals(dto.getId(), domain.getId());
        assertEquals(dto.getCsvId(), domain.getCsvId());
        assertEquals(dto.getRecipient(), domain.getRecipient());
        assertEquals(dto.getAmount(), domain.getAmount());
        assertEquals(dto.getCurrency(), domain.getCurrency());
        assertEquals(dto.getConversationId(), domain.getConversationId());
        assertEquals(dto.getStatus(), domain.getStatus());
        assertEquals(dto.getMessage(), domain.getMessage());
        assertEquals(dto.getFee(), domain.getFee());
        assertNull(domain.getPaymentStatus());
    }

    // @Test
    // void testDtoToGrpc() {
    //   // Given
    //   PaymentOutputDto dto =
    //       PaymentOutputDto.builder()
    //           .id(UUID.randomUUID())
    //           .csvId("test-record")
    //           .recipient("Test Recipient")
    //           .amount(new BigDecimal("100.50"))
    //           .currency(Currency.getInstance("EUR"))
    //           .conversationId(UUID.randomUUID())
    //           .status(200L)
    //           .message("Success")
    //           .fee(new BigDecimal("1.50"))
    //           .build();

    //   // When
    //   ProcessPaymentStatusSvc.PaymentOutput grpc = mapper.fromDtoToGrpc(dto);

    //   // Then
    //   assertNotNull(grpc);
    //   assertEquals(dto.getId().toString(), grpc.getId());
    //   assertEquals(dto.getCsvId(), grpc.getCsvId());
    //   assertEquals(dto.getRecipient(), grpc.getRecipient());
    //   assertEquals(dto.getAmount().toPlainString(), grpc.getAmount());
    //   assertEquals(dto.getCurrency().getCurrencyCode(), grpc.getCurrency());
    //   assertEquals(dto.getConversationId().toString(), grpc.getConversationId());
    //   assertEquals(dto.getStatus(), grpc.getStatus());
    //   assertEquals(dto.getMessage(), grpc.getMessage());
    //   assertEquals(dto.getFee().toPlainString(), grpc.getFee());
    // }

    @Test
    void testGrpcToDto() {
        // Given
        UUID id = UUID.randomUUID();
        UUID conversationId = UUID.randomUUID();

        ProcessPaymentStatusSvc.PaymentOutput grpc =
                ProcessPaymentStatusSvc.PaymentOutput.newBuilder()
                        .setId(id.toString())
                        .setCsvId("test-record")
                        .setRecipient("Test Recipient")
                        .setAmount("100.50")
                        .setCurrency("EUR")
                        .setConversationId(conversationId.toString())
                        .setStatus(200L)
                        .setMessage("Success")
                        .setFee("1.50")
                        .build();

        // When
        PaymentOutputDto dto = mapper.fromGrpc(grpc);

        // Then
        assertNotNull(dto);
        assertEquals(id, dto.getId());
        assertEquals("test-record", dto.getCsvId());
        assertEquals("Test Recipient", dto.getRecipient());
        assertEquals(new BigDecimal("100.50"), dto.getAmount());
        assertEquals(Currency.getInstance("EUR"), dto.getCurrency());
        assertEquals(conversationId, dto.getConversationId());
        assertEquals(200L, dto.getStatus());
        assertEquals("Success", dto.getMessage());
        assertEquals(new BigDecimal("1.50"), dto.getFee());
        assertNull(dto.getPaymentStatus());
    }

    // @Test
    // void testDomainToGrpc() {
    //   // Given
    //   PaymentOutput domain = new PaymentOutput();
    //   domain.setId(UUID.randomUUID());
    //   domain.setCsvId("test-record");
    //   domain.setRecipient("Test Recipient");
    //   domain.setAmount(new BigDecimal("100.50"));
    //   domain.setCurrency(Currency.getInstance("EUR"));
    //   domain.setConversationId(UUID.randomUUID());
    //   domain.setStatus(200L);
    //   domain.setMessage("Success");
    //   domain.setFee(new BigDecimal("1.50"));

    //   // When
    //   ProcessPaymentStatusSvc.PaymentOutput grpc = mapper.toDtoToGrpc(domain);

    //   // Then
    //   assertNotNull(grpc);
    //   assertEquals(domain.getId().toString(), grpc.getId());
    //   assertEquals(domain.getCsvId(), grpc.getCsvId());
    //   assertEquals(domain.getRecipient(), grpc.getRecipient());
    //   assertEquals(domain.getAmount().toPlainString(), grpc.getAmount());
    //   assertEquals(domain.getCurrency().getCurrencyCode(), grpc.getCurrency());
    //   assertEquals(domain.getConversationId().toString(), grpc.getConversationId());
    //   assertEquals(domain.getStatus(), grpc.getStatus());
    //   assertEquals(domain.getMessage(), grpc.getMessage());
    //   assertEquals(domain.getFee().toPlainString(), grpc.getFee());
    // }

    @Test
    void testGrpcToDomain() {
        // Given
        UUID id = UUID.randomUUID();
        UUID conversationId = UUID.randomUUID();

        ProcessPaymentStatusSvc.PaymentOutput grpc =
                ProcessPaymentStatusSvc.PaymentOutput.newBuilder()
                        .setId(id.toString())
                        .setCsvId("test-record")
                        .setRecipient("Test Recipient")
                        .setAmount("100.50")
                        .setCurrency("EUR")
                        .setConversationId(conversationId.toString())
                        .setStatus(200L)
                        .setMessage("Success")
                        .setFee("1.50")
                        .build();

        // When
        PaymentOutput domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain);
        assertEquals(id, domain.getId());
        assertEquals("test-record", domain.getCsvId());
        assertEquals("Test Recipient", domain.getRecipient());
        assertEquals(new BigDecimal("100.50"), domain.getAmount());
        assertEquals(Currency.getInstance("EUR"), domain.getCurrency());
        assertEquals(conversationId, domain.getConversationId());
        assertEquals(200L, domain.getStatus());
        assertEquals("Success", domain.getMessage());
        assertEquals(new BigDecimal("1.50"), domain.getFee());
        assertNull(domain.getPaymentStatus());
    }

    @Test
    void testSerializeDeserialize() throws Exception {
        // Build DTO
        PaymentOutputDto dto =
                PaymentOutputDto.builder()
                        .id(UUID.randomUUID())
                        .csvId("test-record")
                        .recipient("Test Recipient")
                        .amount(new BigDecimal("100.50"))
                        .currency(Currency.getInstance("EUR"))
                        .conversationId(UUID.randomUUID())
                        .status(200L)
                        .message("Success")
                        .fee(new BigDecimal("1.50"))
                        .build();

        ObjectMapper mapper = new ObjectMapper();

        // Serialize to JSON
        String json = mapper.writeValueAsString(dto);

        // Deserialize back
        PaymentOutputDto deserialized = mapper.readValue(json, PaymentOutputDto.class);

        // Assert equality
        assertEquals(dto, deserialized);
    }
}
