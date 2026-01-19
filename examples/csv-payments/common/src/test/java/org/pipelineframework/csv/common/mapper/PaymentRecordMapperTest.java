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
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.dto.PaymentRecordDto;
import org.pipelineframework.csv.grpc.ProcessCsvPaymentsInputSvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PaymentRecordMapperTest {

    private PaymentRecordMapper mapper;
    private CommonConverters commonConverters;

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

            mapper = paymentRecordMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set PaymentRecordMapper dependencies", e);
        }
    }

    // Create a test entity
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
        PaymentRecord domain = createTestPaymentRecord();

        // When
        PaymentRecordDto dto = mapper.toDto(domain);

        // Then
        assertNotNull(dto);
        assertEquals(domain.getId(), dto.getId());
        assertEquals(domain.getCsvId(), dto.getCsvId());
        assertEquals(domain.getRecipient(), dto.getRecipient());
        assertEquals(domain.getAmount(), dto.getAmount());
        assertEquals(domain.getCurrency(), dto.getCurrency());
        assertEquals(domain.getCsvPaymentsInputFilePath(), dto.getCsvPaymentsInputFilePath());
    }

    @Test
    void testDtoToDomain() {
        // Given
        PaymentRecordDto dto =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId("test-record")
                        .recipient("Test Recipient")
                        .amount(new BigDecimal("100.50"))
                        .currency(Currency.getInstance("EUR"))
                        .csvPaymentsInputFilePath(Path.of("/test/path/file.csv"))
                        .build();

        // When
        PaymentRecord domain = mapper.fromDto(dto);

        // Then
        assertNotNull(domain);
        assertEquals(dto.getId(), domain.getId());
        assertEquals(dto.getCsvId(), domain.getCsvId());
        assertEquals(dto.getRecipient(), domain.getRecipient());
        assertEquals(dto.getAmount(), domain.getAmount());
        assertEquals(dto.getCurrency(), domain.getCurrency());
        assertEquals(dto.getCsvPaymentsInputFilePath(), domain.getCsvPaymentsInputFilePath());
    }

    @Test
    void testDtoToGrpc() {
        // Given
        PaymentRecordDto dto =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId("test-record")
                        .recipient("Test Recipient")
                        .amount(new BigDecimal("100.50"))
                        .currency(Currency.getInstance("EUR"))
                        .csvPaymentsInputFilePath(Path.of("/test/path/file.csv"))
                        .build();

        // When
        ProcessCsvPaymentsInputSvc.PaymentRecord grpc = mapper.toGrpc(dto);

        // Then
        assertNotNull(grpc);
        assertEquals(dto.getId().toString(), grpc.getId());
        assertEquals(dto.getCsvId(), grpc.getCsvId());
        assertEquals(dto.getRecipient(), grpc.getRecipient());
        assertEquals(dto.getAmount().toPlainString(), grpc.getAmount());
        assertEquals(dto.getCurrency().getCurrencyCode(), grpc.getCurrency());
        assertEquals(
                dto.getCsvPaymentsInputFilePath().toString(), grpc.getCsvPaymentsInputFilePath());
    }

    @Test
    void testGrpcToDto() {
        // Given
        UUID id = UUID.randomUUID();

        ProcessCsvPaymentsInputSvc.PaymentRecord grpc =
                ProcessCsvPaymentsInputSvc.PaymentRecord.newBuilder()
                        .setId(id.toString())
                        .setCsvId("test-record")
                        .setRecipient("Test Recipient")
                        .setAmount("100.50")
                        .setCurrency("EUR")
                        .setCsvPaymentsInputFilePath("/test/path/file.csv")
                        .build();

        // When
        PaymentRecordDto dto = mapper.fromGrpc(grpc);

        // Then
        assertNotNull(dto);
        assertEquals(id, dto.getId());
        assertEquals("test-record", dto.getCsvId());
        assertEquals("Test Recipient", dto.getRecipient());
        assertEquals(new BigDecimal("100.50"), dto.getAmount());
        assertEquals(Currency.getInstance("EUR"), dto.getCurrency());
        assertEquals(Path.of("/test/path/file.csv"), dto.getCsvPaymentsInputFilePath());
    }

    @Test
    void testDomainToGrpc() {
        // Given
        PaymentRecord domain = createTestPaymentRecord();

        // When
        ProcessCsvPaymentsInputSvc.PaymentRecord grpc = mapper.toDtoToGrpc(domain);

        // Then
        assertNotNull(grpc);
        assertEquals(domain.getId().toString(), grpc.getId());
        assertEquals(domain.getCsvId(), grpc.getCsvId());
        assertEquals(domain.getRecipient(), grpc.getRecipient());
        assertEquals(domain.getAmount().toPlainString(), grpc.getAmount());
        assertEquals(domain.getCurrency().getCurrencyCode(), grpc.getCurrency());
        assertEquals(
                domain.getCsvPaymentsInputFilePath().toString(),
                grpc.getCsvPaymentsInputFilePath());
    }

    @Test
    void testGrpcToDomain() {
        // Given
        UUID id = UUID.randomUUID();

        ProcessCsvPaymentsInputSvc.PaymentRecord grpc =
                ProcessCsvPaymentsInputSvc.PaymentRecord.newBuilder()
                        .setId(id.toString())
                        .setCsvId("test-record")
                        .setRecipient("Test Recipient")
                        .setAmount("100.50")
                        .setCurrency("EUR")
                        .setCsvPaymentsInputFilePath("/test/path/file.csv")
                        .build();

        // When
        PaymentRecord domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain);
        assertEquals(id, domain.getId());
        assertEquals("test-record", domain.getCsvId());
        assertEquals("Test Recipient", domain.getRecipient());
        assertEquals(new BigDecimal("100.50"), domain.getAmount());
        assertEquals(Currency.getInstance("EUR"), domain.getCurrency());
        assertEquals(Path.of("/test/path/file.csv"), domain.getCsvPaymentsInputFilePath());
    }

    @Test
    void testSerializeDeserialize() throws Exception {
        // Build DTO
        PaymentRecordDto dto =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId("test-record")
                        .recipient("Test Recipient")
                        .amount(new BigDecimal("100.50"))
                        .currency(Currency.getInstance("EUR"))
                        .csvPaymentsInputFilePath(Path.of("/test/path/file.csv"))
                        .build();

        ObjectMapper mapper = new ObjectMapper();

        // Serialize to JSON
        String json = mapper.writeValueAsString(dto);

        // Deserialize back
        PaymentRecordDto deserialized = mapper.readValue(json, PaymentRecordDto.class);

        // Assert equality
        assertEquals(dto, deserialized);
    }
}
