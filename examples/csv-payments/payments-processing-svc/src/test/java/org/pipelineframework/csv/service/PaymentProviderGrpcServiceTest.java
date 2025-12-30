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
import io.smallrye.mutiny.Uni;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.dto.PaymentRecordDto;
import org.pipelineframework.csv.common.mapper.AckPaymentSentMapper;
import org.pipelineframework.csv.common.mapper.PaymentRecordMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;
import org.pipelineframework.csv.common.mapper.SendPaymentRequestMapper;
import org.pipelineframework.csv.grpc.PaymentsProcessingSvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class PaymentProviderGrpcServiceTest {

    @Mock private PaymentProviderService domainService;

    @Mock private SendPaymentRequestMapper sendPaymentRequestMapper;

    @Mock private AckPaymentSentMapper ackPaymentSentMapper;

    @Mock private PaymentStatusMapper paymentStatusMapper;

    @InjectMocks private PaymentProviderGrpcService paymentProviderGrpcService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("sendPayment: Should successfully process request and return AckPaymentSent")
    void sendPayment_happyPath() {
        // Given
        org.pipelineframework.csv.grpc.PaymentsProcessingSvc.SendPaymentRequest grpcRequest =
                org.pipelineframework.csv.grpc.PaymentsProcessingSvc.SendPaymentRequest.newBuilder()
                        .setAmount("100.00")
                        .setCurrency("USD")
                        .setReference("John Doe")
                        .setPaymentRecordId(UUID.randomUUID().toString())
                        .build();

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

        SendPaymentRequestMapper.SendPaymentRequest request =
                new SendPaymentRequestMapper.SendPaymentRequest()
                        .setAmount(paymentRecord.getAmount())
                        .setReference(paymentRecord.getRecipient())
                        .setCurrency(paymentRecord.getCurrency())
                        .setPaymentRecord(paymentRecord)
                        .setPaymentRecordId(paymentRecord.getId());

        AckPaymentSent domainOut = new AckPaymentSent();
        domainOut.setPaymentRecordId(paymentRecord.getId());
        domainOut.setStatus(1000L);
        domainOut.setMessage("OK");

        PaymentsProcessingSvc.AckPaymentSent grpcResponse =
                PaymentsProcessingSvc.AckPaymentSent.newBuilder()
                        .setConversationId("convId")
                        .setPaymentRecordId(paymentRecord.getId().toString())
                        .setStatus(1000L)
                        .setMessage("OK")
                        .build();

        when(sendPaymentRequestMapper.fromGrpc(
                        any(
                                org.pipelineframework.csv.grpc.PaymentsProcessingSvc.SendPaymentRequest
                                        .class)))
                .thenReturn(request);
        when(domainService.sendPayment(request)).thenReturn(domainOut);
        when(ackPaymentSentMapper.toDtoToGrpc(domainOut)).thenReturn(grpcResponse);

        // When
        Uni<PaymentsProcessingSvc.AckPaymentSent> resultUni =
                paymentProviderGrpcService.sendPayment(grpcRequest);

        // Then
        PaymentsProcessingSvc.AckPaymentSent actualResponse = resultUni.await().indefinitely();
        assertThat(actualResponse).isEqualTo(grpcResponse);
    }

    @Test
    @DisplayName("sendPayment: Should throw StatusRuntimeException on domain service error")
    void sendPayment_domainServiceError_shouldThrowStatusRuntimeException() {
        // Given
        org.pipelineframework.csv.grpc.PaymentsProcessingSvc.SendPaymentRequest grpcRequest =
                org.pipelineframework.csv.grpc.PaymentsProcessingSvc.SendPaymentRequest.newBuilder()
                        .setAmount("100.00")
                        .setCurrency("USD")
                        .setReference("John Doe")
                        .setPaymentRecordId(UUID.randomUUID().toString())
                        .build();

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

        SendPaymentRequestMapper.SendPaymentRequest request =
                new SendPaymentRequestMapper.SendPaymentRequest()
                        .setAmount(paymentRecord.getAmount())
                        .setReference(paymentRecord.getRecipient())
                        .setCurrency(paymentRecord.getCurrency())
                        .setPaymentRecord(paymentRecord)
                        .setPaymentRecordId(paymentRecord.getId());

        RuntimeException domainException = new RuntimeException("Domain service failed");

        when(sendPaymentRequestMapper.fromGrpc(
                        any(
                                org.pipelineframework.csv.grpc.PaymentsProcessingSvc.SendPaymentRequest
                                        .class)))
                .thenThrow(domainException);
        when(domainService.sendPayment(request)).thenThrow(domainException);

        // When & Then
        StatusRuntimeException thrown =
                assertThrows(
                        StatusRuntimeException.class,
                        () ->
                                paymentProviderGrpcService
                                        .sendPayment(grpcRequest)
                                        .await()
                                        .indefinitely());
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(thrown.getStatus().getDescription()).contains("Domain service failed");
        assertThat(thrown.getCause()).isEqualTo(domainException);
    }

    @Test
    @DisplayName("getPaymentStatus: Should successfully process request and return PaymentStatus")
    @SneakyThrows
    void getPaymentStatus_happyPath() {
        // Given
        PaymentsProcessingSvc.AckPaymentSent grpcRequest =
                PaymentsProcessingSvc.AckPaymentSent.newBuilder()
                        .setConversationId("convId")
                        .setPaymentRecordId(UUID.randomUUID().toString())
                        .build();

        AckPaymentSent ackPaymentSent = new AckPaymentSent();
        ackPaymentSent.setPaymentRecordId(UUID.randomUUID());

        PaymentStatus domainOut = new PaymentStatus();
        domainOut.setAckPaymentSentId(ackPaymentSent.getId());

        PaymentsProcessingSvc.PaymentStatus grpcResponse =
                PaymentsProcessingSvc.PaymentStatus.newBuilder().setReference("ref").build();

        when(ackPaymentSentMapper.fromGrpcFromDto(grpcRequest)).thenReturn(ackPaymentSent);
        when(domainService.getPaymentStatus(ackPaymentSent)).thenReturn(domainOut);
        when(paymentStatusMapper.toDtoToGrpc(domainOut)).thenReturn(grpcResponse);

        // When
        Uni<PaymentsProcessingSvc.PaymentStatus> resultUni =
                paymentProviderGrpcService.getPaymentStatus(grpcRequest);

        // Then
        PaymentsProcessingSvc.PaymentStatus actualResponse = resultUni.await().indefinitely();
        assertThat(actualResponse).isEqualTo(grpcResponse);
    }

    @Test
    @DisplayName("getPaymentStatus: Should throw StatusRuntimeException on domain service error")
    @SneakyThrows
    void getPaymentStatus_domainServiceError_shouldThrowStatusRuntimeException() {
        // Given
        PaymentsProcessingSvc.AckPaymentSent grpcRequest =
                PaymentsProcessingSvc.AckPaymentSent.newBuilder()
                        .setConversationId("convId")
                        .setPaymentRecordId(UUID.randomUUID().toString())
                        .build();

        AckPaymentSent domainIn = new AckPaymentSent();
        domainIn.setPaymentRecordId(UUID.randomUUID());

        RuntimeException domainException = new RuntimeException("Domain service failed");

        when(ackPaymentSentMapper.fromGrpcFromDto(grpcRequest)).thenReturn(domainIn);
        when(domainService.getPaymentStatus(domainIn)).thenThrow(domainException);

        // When & Then
        StatusRuntimeException thrown =
                assertThrows(
                        StatusRuntimeException.class,
                        () ->
                                paymentProviderGrpcService
                                        .getPaymentStatus(grpcRequest)
                                        .await()
                                        .indefinitely());
        assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(thrown.getStatus().getDescription()).contains("Processing failed");
        assertThat(thrown.getCause()).isEqualTo(domainException);
    }
}
