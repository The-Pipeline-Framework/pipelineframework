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

import java.math.BigDecimal;
import java.util.UUID;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.domain.SendPaymentRequest;
import org.pipelineframework.csv.common.dto.PaymentRecordDto;
import org.pipelineframework.csv.common.mapper.AckPaymentSentMapper;
import org.pipelineframework.csv.common.mapper.PaymentRecordMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaymentProviderServiceMockTest {

    PaymentProviderServiceMock paymentProviderServiceMock;
    private AckPaymentSentMapper ackPaymentSentMapper;
    private PaymentStatusMapper paymentStatusMapper;

    @BeforeEach
    void setUp() {
        ackPaymentSentMapper = mock(AckPaymentSentMapper.class);
        paymentStatusMapper = mock(PaymentStatusMapper.class);
    }

    @Test
    @DisplayName("Should successfully send payment and return AckPaymentSent")
    void sendPayment_happyPath_shouldReturnAckPaymentSent() {
        // Given
        PaymentRecordDto dtoIn =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId(String.valueOf(UUID.randomUUID()))
                        .recipient("John Doe")
                        .amount(BigDecimal.valueOf(100.00))
                        .currency(java.util.Currency.getInstance("USD"))
                        .build();
        PaymentRecordMapper paymentRecordMapper = Mappers.getMapper(PaymentRecordMapper.class);
        PaymentRecord paymentRecord = paymentRecordMapper.fromDto(dtoIn);

        SendPaymentRequest request =
                new SendPaymentRequest()
                        .setAmount(paymentRecord.getAmount())
                        .setReference(paymentRecord.getRecipient())
                        .setCurrency(paymentRecord.getCurrency())
                        .setPaymentRecord(paymentRecord)
                        .setPaymentRecordId(paymentRecord.getId());

        PaymentProviderConfig config = new FakePaymentProviderConfig();
        PaymentProviderServiceMock paymentProviderServiceMock =
                new PaymentProviderServiceMock(config);

        new PaymentProviderServiceMock(config);
        AckPaymentSent testAckPaymentSent =
                new AckPaymentSent(UUID.randomUUID())
                        .setStatus(1000L)
                        .setMessage("OK but this is only a test")
                        .setPaymentRecordId(paymentRecord.getId())
                        .setPaymentRecord(paymentRecord);
        when(ackPaymentSentMapper.fromDto(any())).thenReturn(testAckPaymentSent);

        AckPaymentSent ackPaymentSent = paymentProviderServiceMock.sendPayment(request);

        // Then
        assertThat(ackPaymentSent).isNotNull();
        assertThat(ackPaymentSent.getStatus()).isEqualTo(1000L);
        assertThat(ackPaymentSent.getMessage()).isEqualTo("OK but this is only a test");
        assertThat(ackPaymentSent.getPaymentRecord()).isEqualTo(paymentRecord);
        assertThat(ackPaymentSent.getPaymentRecordId()).isEqualTo(paymentRecord.getId());
    }

    @Test
    @DisplayName("Should throw StatusRuntimeException when sendPayment is throttled")
    void sendPayment_throttled_shouldThrowException() throws InterruptedException {
        // Given a service with a very low permit rate and no timeout
        PaymentProviderConfig config = new ThrottledPaymentProviderConfig();
        PaymentProviderServiceMock paymentProviderServiceMock =
                new PaymentProviderServiceMock(config);

        // Acquire a permit to ensure the rate limiter is exhausted for subsequent calls
        // RateLimiter.create(rate) allows the first acquire to pass even if rate is very low.
        // So, we need to acquire it once to exhaust it for subsequent calls.
        paymentProviderServiceMock.sendPayment(
                new SendPaymentRequest()
                        .setAmount(new BigDecimal("1.00"))
                        .setReference("dummy")
                        .setCurrency(java.util.Currency.getInstance("USD"))
                        .setPaymentRecordId(UUID.randomUUID()));

        // Ensure enough time passes for the rate limiter to be truly exhausted if it's not already
        Thread.sleep(10); // Small delay to ensure rate limiter state settles

        PaymentRecordDto dtoIn =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId(String.valueOf(UUID.randomUUID()))
                        .recipient("John Doe")
                        .amount(BigDecimal.valueOf(100.00))
                        .currency(java.util.Currency.getInstance("USD"))
                        .build();
        PaymentRecordMapper paymentRecordMapper = Mappers.getMapper(PaymentRecordMapper.class);
        PaymentRecord paymentRecord = paymentRecordMapper.fromDto(dtoIn);

        SendPaymentRequest request =
                new SendPaymentRequest()
                        .setAmount(paymentRecord.getAmount())
                        .setReference(paymentRecord.getRecipient())
                        .setCurrency(paymentRecord.getCurrency())
                        .setPaymentRecord(paymentRecord)
                        .setPaymentRecordId(paymentRecord.getId());

        // When & Then
        StatusRuntimeException thrown =
                assertThrows(
                        StatusRuntimeException.class,
                        () -> paymentProviderServiceMock.sendPayment(request));
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(thrown.getStatus().getDescription())
                .isEqualTo("Payment service is currently throttled. Please try again later.");
    }

    @Test
    @DisplayName("Should throw StatusRuntimeException when sendPayment timeoutMillis is -1L")
    void sendPayment_timeoutMinusOne_shouldThrowException() {
        // Given a service with timeoutMillis set to -1L
        PaymentProviderConfig config = new NegativeTimeoutPaymentProviderConfig();

        this.paymentProviderServiceMock = new PaymentProviderServiceMock(config);

        PaymentRecordDto dtoIn =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId(String.valueOf(UUID.randomUUID()))
                        .recipient("John Doe")
                        .amount(BigDecimal.valueOf(100.00))
                        .currency(java.util.Currency.getInstance("USD"))
                        .build();
        PaymentRecordMapper paymentRecordMapper = Mappers.getMapper(PaymentRecordMapper.class);
        PaymentRecord paymentRecord = paymentRecordMapper.fromDto(dtoIn);

        SendPaymentRequest request =
                new SendPaymentRequest()
                        .setAmount(paymentRecord.getAmount())
                        .setReference(paymentRecord.getRecipient())
                        .setCurrency(paymentRecord.getCurrency())
                        .setPaymentRecord(paymentRecord)
                        .setPaymentRecordId(paymentRecord.getId());

        // When & Then
        StatusRuntimeException thrown =
                assertThrows(
                        StatusRuntimeException.class,
                        () -> paymentProviderServiceMock.sendPayment(request));
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(thrown.getStatus().getDescription())
                .isEqualTo("Payment service is currently throttled. Please try again later.");
    }

    @Test
    @DisplayName("Should successfully get payment status and return PaymentStatus")
    void getPaymentStatus_happyPath_shouldReturnPaymentStatus() {
        // Given
        PaymentProviderConfig config = new FakePaymentProviderConfig();
        PaymentProviderServiceMock paymentProviderServiceMock =
                new PaymentProviderServiceMock(config);

        // When
        PaymentRecordDto dtoIn =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId(String.valueOf(UUID.randomUUID()))
                        .recipient("John Doe")
                        .amount(BigDecimal.valueOf(100.00))
                        .currency(java.util.Currency.getInstance("USD"))
                        .build();
        PaymentRecordMapper paymentRecordMapper = Mappers.getMapper(PaymentRecordMapper.class);
        PaymentRecord paymentRecord = paymentRecordMapper.fromDto(dtoIn);

        AckPaymentSent testAckPaymentSent =
                new AckPaymentSent(UUID.randomUUID())
                        .setStatus(1000L)
                        .setMessage("OK but this is only a test")
                        .setPaymentRecordId(paymentRecord.getId())
                        .setPaymentRecord(paymentRecord);

        PaymentStatus testPaymentStatus =
                new PaymentStatus()
                        .setReference("101")
                        .setStatus("nada")
                        .setFee(new BigDecimal("1.01"))
                        .setMessage("This is a test")
                        .setAckPaymentSent(testAckPaymentSent)
                        .setAckPaymentSentId(testAckPaymentSent.getId());

        // When
        when(paymentStatusMapper.fromDto(any())).thenReturn(testPaymentStatus);

        PaymentStatus paymentStatus =
                paymentProviderServiceMock.getPaymentStatus(testAckPaymentSent);

        // Then
        assertThat(paymentStatus).isNotNull();
        assertThat(paymentStatus.getReference()).isEqualTo("101");
        assertThat(paymentStatus.getStatus()).isEqualTo("Complete");
        assertThat(paymentStatus.getFee()).isEqualTo(new BigDecimal("1.01"));
        assertThat(paymentStatus.getMessage()).isEqualTo("Mock response");
        assertThat(paymentStatus.getAckPaymentSent()).isEqualTo(testAckPaymentSent);
        assertThat(paymentStatus.getAckPaymentSentId()).isEqualTo(testAckPaymentSent.getId());
    }

    @Test
    @DisplayName("Should throw StatusRuntimeException when getPaymentStatus is throttled")
    void getPaymentStatus_throttled_shouldThrowException() throws InterruptedException {
        // Given a service with a very low permit rate and no timeout
        PaymentProviderConfig config = new LowRatePaymentProviderConfig();
        PaymentProviderServiceMock paymentProviderServiceMock =
                new PaymentProviderServiceMock(config);

        // When
        PaymentRecordDto dtoIn =
                PaymentRecordDto.builder()
                        .id(UUID.randomUUID())
                        .csvId(String.valueOf(UUID.randomUUID()))
                        .recipient("John Doe")
                        .amount(BigDecimal.valueOf(100.00))
                        .currency(java.util.Currency.getInstance("USD"))
                        .build();
        PaymentRecordMapper paymentRecordMapper = Mappers.getMapper(PaymentRecordMapper.class);
        PaymentRecord paymentRecord = paymentRecordMapper.fromDto(dtoIn);

        AckPaymentSent testAckPaymentSent =
                new AckPaymentSent(UUID.randomUUID())
                        .setStatus(1000L)
                        .setMessage("OK but this is only a test")
                        .setPaymentRecordId(paymentRecord.getId())
                        .setPaymentRecord(paymentRecord);

        PaymentStatus testPaymentStatus =
                new PaymentStatus()
                        .setReference("101")
                        .setStatus("nada")
                        .setFee(new BigDecimal("1.01"))
                        .setMessage("This is a test")
                        .setAckPaymentSent(testAckPaymentSent)
                        .setAckPaymentSentId(testAckPaymentSent.getId());

        // When
        when(paymentStatusMapper.fromDto(any())).thenReturn(testPaymentStatus);

        // Acquire a permit to ensure the rate limiter is exhausted for subsequent calls
        try {
            paymentProviderServiceMock.getPaymentStatus(new AckPaymentSent());
        } catch (StatusRuntimeException e) {
            // Expected for the first call if timeoutMillis is 0L and permitsPerSecond is 0.0
        }

        // Ensure enough time passes for the rate limiter to be truly exhausted if it's not already
        Thread.sleep(10); // Small delay to ensure rate limiter state settles

        // When & Then
        StatusRuntimeException thrown =
                assertThrows(
                        StatusRuntimeException.class,
                        () -> paymentProviderServiceMock.getPaymentStatus(testAckPaymentSent));
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(thrown.getStatus().getDescription())
                .isEqualTo(
                        "Failed to acquire permit within timeout period. The payment status service is currently throttled.");
    }

    static class FakePaymentProviderConfig implements PaymentProviderConfig {
        @Override
        public double permitsPerSecond() {
            return 1000.0;
        }

        @Override
        public long timeoutMillis() {
            return 5000;
        }

        @Override
        public double waitMilliseconds() {
            return 50.0;
        }
    }

    private static class ThrottledPaymentProviderConfig implements PaymentProviderConfig {
        @Override
        public double permitsPerSecond() {
            return 0.001;
        }

        @Override
        public long timeoutMillis() {
            return 5000;
        }

        @Override
        public double waitMilliseconds() {
            return 50.0;
        }
    }

    private static class NegativeTimeoutPaymentProviderConfig implements PaymentProviderConfig {
        @Override
        public double permitsPerSecond() {
            return 100.0;
        }

        @Override
        public long timeoutMillis() {
            return -1L;
        }

        @Override
        public double waitMilliseconds() {
            return 50.0;
        }
    }

    private static class LowRatePaymentProviderConfig implements PaymentProviderConfig {
        @Override
        public double permitsPerSecond() {
            return 0.001;
        }

        @Override
        public long timeoutMillis() {
            return 0;
        }

        @Override
        public double waitMilliseconds() {
            return 50.0;
        }
    }
}
