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
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.grpc.PaymentsProcessingSvc;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
class SendPaymentRequestMapperTest {

    @Inject
    SendPaymentRequestMapper mapper;

    @Inject
    PaymentRecordMapper paymentRecordMapper;

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
        PaymentRecord domainRecord = createTestPaymentRecord();
        ProcessCsvPaymentsInputSvc.PaymentRecord grpcRecord =
                paymentRecordMapper.toGrpc(paymentRecordMapper.toDto(domainRecord));
        PaymentsProcessingSvc.SendPaymentRequest grpc =
                PaymentsProcessingSvc.SendPaymentRequest.newBuilder()
                        .setMsisdn("123456789")
                        .setAmount("100.50")
                        .setCurrency("EUR")
                        .setReference("test-ref")
                        .setUrl("http://test.com")
                        .setPaymentRecordId(UUID.randomUUID().toString())
                        .setPaymentRecord(grpcRecord)
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
        assertEquals(domainRecord.getCsvPaymentsInputFilePath(), domain.getPaymentRecord().getCsvPaymentsInputFilePath());
    }

    @Test
    void testGrpcToDomainMapsPaymentRecord() {
        // Given
        PaymentRecord domainRecord = createTestPaymentRecord();
        ProcessCsvPaymentsInputSvc.PaymentRecord grpcRecord =
                paymentRecordMapper.toGrpc(paymentRecordMapper.toDto(domainRecord));
        PaymentsProcessingSvc.SendPaymentRequest grpc =
                PaymentsProcessingSvc.SendPaymentRequest.newBuilder()
                        .setMsisdn("123456789")
                        .setAmount("100.50")
                        .setCurrency("EUR")
                        .setReference("test-ref")
                        .setUrl("http://test.com")
                        .setPaymentRecordId(domainRecord.getId().toString())
                        .setPaymentRecord(grpcRecord)
                        .build();

        // When
        SendPaymentRequestMapper.SendPaymentRequest domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain.getPaymentRecord());
        assertEquals(domainRecord.getId(), domain.getPaymentRecord().getId());
        assertEquals(domainRecord.getCsvId(), domain.getPaymentRecord().getCsvId());
        assertEquals(domainRecord.getRecipient(), domain.getPaymentRecord().getRecipient());
        assertEquals(domainRecord.getAmount(), domain.getPaymentRecord().getAmount());
        assertEquals(domainRecord.getCurrency(), domain.getPaymentRecord().getCurrency());
        assertEquals(domainRecord.getCsvPaymentsInputFilePath(), domain.getPaymentRecord().getCsvPaymentsInputFilePath());
    }

    @Test
    void testDomainToGrpcMapsPaymentRecord() {
        // Given
        PaymentRecord domainRecord = createTestPaymentRecord();
        SendPaymentRequestMapper.SendPaymentRequest domain =
                new SendPaymentRequestMapper.SendPaymentRequest()
                        .setMsisdn("123456789")
                        .setAmount(new BigDecimal("100.50"))
                        .setCurrency(Currency.getInstance("EUR"))
                        .setReference("test-ref")
                        .setUrl("http://test.com")
                        .setPaymentRecordId(domainRecord.getId())
                        .setPaymentRecord(domainRecord);

        // When
        PaymentsProcessingSvc.SendPaymentRequest grpc = mapper.toDtoToGrpc(domain);

        // Then
        assertNotNull(grpc.getPaymentRecord());
        assertEquals(domainRecord.getId().toString(), grpc.getPaymentRecord().getId());
        assertEquals(domainRecord.getCsvId(), grpc.getPaymentRecord().getCsvId());
        assertEquals(domainRecord.getRecipient(), grpc.getPaymentRecord().getRecipient());
        assertEquals(domainRecord.getAmount().toPlainString(), grpc.getPaymentRecord().getAmount());
        assertEquals(domainRecord.getCurrency().getCurrencyCode(), grpc.getPaymentRecord().getCurrency());
        assertEquals(domainRecord.getCsvPaymentsInputFilePath().toString(), grpc.getPaymentRecord().getCsvPaymentsInputFilePath());
    }
}
