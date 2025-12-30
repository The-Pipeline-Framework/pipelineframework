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
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.grpc.PaymentsProcessingSvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SendPaymentRequestMapperTest {

    private SendPaymentRequestMapper mapper;
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

        // Create SendPaymentRequestMapperImpl and set its dependencies
        SendPaymentRequestMapperImpl sendPaymentRequestMapperImpl =
                new SendPaymentRequestMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    SendPaymentRequestMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(sendPaymentRequestMapperImpl, commonConverters);

            java.lang.reflect.Field paymentRecordMapperField =
                    SendPaymentRequestMapperImpl.class.getDeclaredField("paymentRecordMapper");
            paymentRecordMapperField.setAccessible(true);
            paymentRecordMapperField.set(sendPaymentRequestMapperImpl, paymentRecordMapper);

            mapper = sendPaymentRequestMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set SendPaymentRequestMapper dependencies", e);
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
    void testGrpcToDomain() {
        // Given
        PaymentsProcessingSvc.SendPaymentRequest grpc =
                PaymentsProcessingSvc.SendPaymentRequest.newBuilder()
                        .setMsisdn("123456789")
                        .setAmount("100.50")
                        .setCurrency("EUR")
                        .setReference("test-ref")
                        .setUrl("http://test.com")
                        .setPaymentRecordId(UUID.randomUUID().toString())
                        .build();

        // When
        SendPaymentRequestMapper.SendPaymentRequest domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain);
        assertEquals("123456789", domain.getMsisdn());
        assertEquals(new BigDecimal("100.50"), domain.getAmount());
        assertEquals(Currency.getInstance("EUR"), domain.getCurrency());
        assertEquals("test-ref", domain.getReference());
        assertEquals("http://test.com", domain.getUrl());
        assertNotNull(domain.getPaymentRecordId());
    }

    // @Test
    // void testDomainToGrpc() {
    //   // Given
    //   SendPaymentRequestMapper.SendPaymentRequest domain =
    //       new SendPaymentRequestMapper.SendPaymentRequest();
    //   domain.setMsisdn("123456789");
    //   domain.setAmount(new BigDecimal("100.50"));
    //   domain.setCurrency(Currency.getInstance("EUR"));
    //   domain.setReference("test-ref");
    //   domain.setUrl("http://test.com");
    //   domain.setPaymentRecordId(UUID.randomUUID());

    //   // When
    //   PaymentStatusSvc.SendPaymentRequest grpc = mapper.toDtoToGrpc(domain);

    //   // Then
    //   assertNotNull(grpc);
    //   assertEquals("123456789", grpc.getMsisdn());
    //   assertEquals("100.50", grpc.getAmount());
    //   assertEquals("EUR", grpc.getCurrency());
    //   assertEquals("test-ref", grpc.getReference());
    //   assertEquals("http://test.com", grpc.getUrl());
    //   assertNotNull(grpc.getPaymentRecordId());
    // }

    // @Test
    // void testSerializeDeserialize() throws Exception {
    //   // Build a simple DTO-like object for testing
    //   PaymentStatusSvc.SendPaymentRequest.Builder builder =
    //       PaymentStatusSvc.SendPaymentRequest.newBuilder()
    //           .setMsisdn("123456789")
    //           .setAmount("100.50")
    //           .setCurrency("EUR")
    //           .setReference("test-ref")
    //           .setUrl("http://test.com")
    //           .setPaymentRecordId(UUID.randomUUID().toString());
    //
    //   PaymentStatusSvc.SendPaymentRequest request = builder.build();
    //
    //   ObjectMapper mapper = new ObjectMapper();
    //
    //   // Serialize to JSON
    //   String json = mapper.writeValueAsString(request);
    //
    //   // Deserialize back
    //   PaymentStatusSvc.SendPaymentRequest deserialized =
    //       mapper.readValue(json, PaymentStatusSvc.SendPaymentRequest.class);
    //
    //   // Assert equality
    //   assertEquals(request, deserialized);
    // }
}
