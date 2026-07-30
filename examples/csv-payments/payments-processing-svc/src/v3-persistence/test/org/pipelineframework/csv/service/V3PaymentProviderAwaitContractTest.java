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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.util.JsonFormat;
import io.smallrye.mutiny.Uni;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;
import java.util.Currency;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.pipelineframework.awaitable.kafka.KafkaAwaitDispatchEnvelope;
import org.pipelineframework.awaitable.kafka.KafkaAwaitCompletionEnvelope;
import org.pipelineframework.awaitable.AwaitPayloadSupport;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.csv.domain.PaymentRecord;
import org.pipelineframework.csv.domain.PaymentStatus;
import org.pipelineframework.csv.domain.PipelineDomainProtoAdapters;
import org.pipelineframework.csv.grpc.PipelineTypes;
import io.smallrye.reactive.messaging.MutinyEmitter;

class V3PaymentProviderAwaitContractTest {

    @Test
    void generatedAwaitPayloadRoundTripsThroughTheProviderBoundary() {
        PaymentRecord record = new PaymentRecord(
            UUID.randomUUID(), "csv-1", "Ada", new BigDecimal("12.34"),
            Currency.getInstance("EUR"), Path.of("/tmp/payments.csv"));
        var provider = new PaymentProviderServiceMock(new PaymentProviderServiceMockTest.FakePaymentProviderConfig());

        PaymentStatus status = provider.processPayment(
            PipelineDomainProtoAdapters.fromProto(PipelineDomainProtoAdapters.toProto(record)));

        PaymentStatus rebuilt = PipelineDomainProtoAdapters.fromProto(PipelineDomainProtoAdapters.toProto(status));
        var approved = assertInstanceOf(PaymentStatus.Approved.class, rebuilt);
        assertEquals(record.id(), approved.value().paymentRecordId());
        assertEquals(record, approved.value().paymentRecord());
    }

    @Test
    void kafkaAwaitDispatchConvertsV3PaymentRecordJson() throws Exception {
        PaymentRecord record = new PaymentRecord(
            UUID.randomUUID(), "csv-1", "Ada", new BigDecimal("12.34"),
            Currency.getInstance("EUR"), Path.of("/tmp/payments.csv"));
        MutinyEmitter<String> results = mock(MutinyEmitter.class);
        when(results.send(anyString())).thenReturn(Uni.createFrom().voidItem());
        PaymentProviderKafkaAwaitMock awaitMock = configuredKafkaAwaitMock(results);
        Message<String> message = acknowledgedMessage(dispatchJson(protobufJsonPayload(record)));

        awaitMock.consume(message).toCompletableFuture().join();

        verify(message).ack();
        verify(results).send(anyString());
    }

    @Test
    void kafkaAwaitCompletionPreservesTheV3PaymentStatusUnionArm() throws Exception {
        PaymentRecord record = new PaymentRecord(
            UUID.randomUUID(), "csv-1", "Ada", new BigDecimal("12.34"),
            Currency.getInstance("EUR"), Path.of("/tmp/payments.csv"));
        MutinyEmitter<String> results = mock(MutinyEmitter.class);
        when(results.send(anyString())).thenReturn(Uni.createFrom().voidItem());
        PaymentProviderKafkaAwaitMock awaitMock = configuredKafkaAwaitMock(results);

        awaitMock.consume(acknowledgedMessage(dispatchJson(protobufJsonPayload(record)))).toCompletableFuture().join();

        org.mockito.ArgumentCaptor<String> completion = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(results).send(completion.capture());
        KafkaAwaitCompletionEnvelope envelope = PipelineJson.mapper().readValue(
            completion.getValue(), KafkaAwaitCompletionEnvelope.class);
        PipelineTypes.PaymentStatus transport = assertInstanceOf(PipelineTypes.PaymentStatus.class,
            AwaitPayloadSupport.coercePayload(envelope.responsePayload(), PipelineTypes.PaymentStatus.class));
        assertTrue(transport.hasApproved());
        assertInstanceOf(PaymentStatus.Approved.class, PipelineDomainProtoAdapters.fromProto(transport));
    }

    @Test
    void kafkaAwaitDispatchRejectsMismatchedV3PaymentRecordFields() throws Exception {
        MutinyEmitter<String> results = mock(MutinyEmitter.class);
        PaymentProviderKafkaAwaitMock awaitMock = configuredKafkaAwaitMock(results);
        Message<String> message = rejectedMessage(dispatchJson(Map.of(
            "paymentRecordId", UUID.randomUUID().toString(),
            "paymentAmount", "12.34")));

        assertThrows(CompletionException.class, () -> awaitMock.consume(message).toCompletableFuture().join());

        verify(message).nack(any());
        verify(results, never()).send(anyString());
    }

    private static PaymentProviderKafkaAwaitMock configuredKafkaAwaitMock(MutinyEmitter<String> results) {
        PaymentProviderKafkaAwaitMock awaitMock = new PaymentProviderKafkaAwaitMock();
        awaitMock.paymentProvider = new PaymentProviderServiceMock(new PaymentProviderServiceMockTest.FakePaymentProviderConfig());
        awaitMock.paymentProviderConfig = new PaymentProviderServiceMockTest.FakePaymentProviderConfig();
        awaitMock.results = results;
        return awaitMock;
    }

    private static Message<String> acknowledgedMessage(String payload) {
        Message<String> message = mock(Message.class);
        when(message.getPayload()).thenReturn(payload);
        when(message.ack()).thenReturn(CompletableFuture.completedFuture(null));
        return message;
    }

    private static Message<String> rejectedMessage(String payload) {
        Message<String> message = mock(Message.class);
        when(message.getPayload()).thenReturn(payload);
        when(message.nack(any())).thenAnswer(invocation -> CompletableFuture.failedFuture(invocation.getArgument(0)));
        return message;
    }

    private static Object protobufJsonPayload(PaymentRecord record) throws Exception {
        return PipelineJson.mapper().readValue(
            JsonFormat.printer().print(PipelineDomainProtoAdapters.toProto(record)), Object.class);
    }

    private static String dispatchJson(Object requestPayload) throws Exception {
        return PipelineJson.mapper().writeValueAsString(new KafkaAwaitDispatchEnvelope(
            "tenant-1",
            "execution-1",
            "interaction-1",
            "correlation-1",
            "AwaitPaymentProvider",
            System.currentTimeMillis() + 60_000L,
            PaymentRecord.class.getName(),
            PaymentStatus.class.getName(),
            "resume-token",
            requestPayload,
            Map.of()));
    }
}
