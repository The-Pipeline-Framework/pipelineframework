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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.awaitable.kafka.KafkaAwaitCompletionEnvelope;
import org.pipelineframework.awaitable.kafka.KafkaAwaitDispatchEnvelope;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;

class PaymentProviderKafkaAwaitMockTest {

  @Mock
  PaymentProviderServiceMock paymentProvider;

  PaymentProviderConfig paymentProviderConfig;

  @Mock
  MutinyEmitter<String> results;

  PaymentProviderKafkaAwaitMock mockProvider;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    paymentProviderConfig = new PaymentProviderServiceMockTest.FakePaymentProviderConfig();
    mockProvider = new PaymentProviderKafkaAwaitMock();
    mockProvider.paymentProvider = paymentProvider;
    mockProvider.paymentProviderConfig = paymentProviderConfig;
    mockProvider.results = results;
  }

  @Test
  void consumesDispatchEnvelopeAndPublishesCompletionEnvelope() throws Exception {
    PaymentRecord paymentRecord = validPaymentRecord();
    PaymentStatus status = validPaymentStatus(paymentRecord);
    when(paymentProvider.processPayment(any(PaymentRecord.class))).thenReturn(status);
    when(results.send(anyString())).thenReturn(Uni.createFrom().voidItem());

    mockProvider.consume(Message.of(dispatchJson(paymentRecord)))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);

    ArgumentCaptor<PaymentRecord> requestCaptor = ArgumentCaptor.forClass(PaymentRecord.class);
    verify(paymentProvider).processPayment(requestCaptor.capture());
    assertEquals(paymentRecord.getAmount(), requestCaptor.getValue().getAmount());
    assertEquals(paymentRecord.getRecipient(), requestCaptor.getValue().getRecipient());
    assertEquals(paymentRecord.getCurrency(), requestCaptor.getValue().getCurrency());

    ArgumentCaptor<String> completionCaptor = ArgumentCaptor.forClass(String.class);
    verify(results).send(completionCaptor.capture());
    KafkaAwaitCompletionEnvelope completion = PipelineJson.mapper()
        .readValue(completionCaptor.getValue(), KafkaAwaitCompletionEnvelope.class);
    assertEquals("tenant-1", completion.tenantId());
    assertEquals("interaction-1", completion.interactionId());
    assertEquals("corr-1", completion.correlationId());
    assertEquals("resume-token", completion.resumeToken());
    assertEquals("interaction-1", completion.idempotencyKey());
    assertEquals("csv-payments-mock-provider", completion.actor());
  }

  @Test
  void invalidDispatchPayloadFailsWithoutPublishingCompletion() {
    assertThrows(Exception.class, () -> mockProvider.consume(failingMessage("{not-json"))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS));

    verify(results, never()).send(anyString());
  }

  @Test
  void providerFailureFailsWithoutPublishingCompletion() throws Exception {
    PaymentRecord paymentRecord = validPaymentRecord();
    when(paymentProvider.processPayment(any(PaymentRecord.class))).thenThrow(new IllegalStateException("provider down"));

    assertThrows(Exception.class, () -> mockProvider.consume(failingMessage(dispatchJson(paymentRecord)))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS));

    verify(results, never()).send(anyString());
  }

  @Test
  void sendFailureFailsConsume() throws Exception {
    PaymentRecord paymentRecord = validPaymentRecord();
    when(paymentProvider.processPayment(any(PaymentRecord.class))).thenReturn(validPaymentStatus(paymentRecord));
    when(results.send(anyString())).thenReturn(Uni.createFrom().failure(new IllegalStateException("broker down")));

    assertThrows(Exception.class, () -> mockProvider.consume(failingMessage(dispatchJson(paymentRecord)))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS));
  }

  @Test
  void missingRequiredPaymentFieldsFailsBeforeProviderCall() throws Exception {
    PaymentRecord invalid = new PaymentRecord()
        .setCsvId("csv-1")
        .setRecipient("alice")
        .setCurrency(Currency.getInstance("EUR"));

    assertThrows(Exception.class, () -> mockProvider.consume(failingMessage(dispatchJson(invalid)))
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS));

    verify(paymentProvider, never()).processPayment(any(PaymentRecord.class));
    verify(results, never()).send(anyString());
  }

  private static PaymentRecord validPaymentRecord() {
    PaymentRecord paymentRecord = new PaymentRecord()
        .setCsvId("csv-1")
        .setRecipient("alice")
        .setAmount(new BigDecimal("12.34"))
        .setCurrency(Currency.getInstance("EUR"));
    paymentRecord.setId(UUID.randomUUID());
    return paymentRecord;
  }

  private static PaymentStatus validPaymentStatus(PaymentRecord paymentRecord) {
    ApprovedPaymentStatus paymentStatus = new ApprovedPaymentStatus();
    paymentStatus.setReference("provider-ref");
    paymentStatus.setStatus("Completed");
    paymentStatus.setMessage("settled");
    paymentStatus.setFee(new BigDecimal("0.12"));
    paymentStatus.setConversationId(UUID.randomUUID());
    paymentStatus.setStatusCode(1000L);
    paymentStatus.setPaymentRecord(paymentRecord);
    paymentStatus.setPaymentRecordId(paymentRecord.getId());
    return paymentStatus;
  }

  private static String dispatchJson(PaymentRecord paymentRecord) throws Exception {
    KafkaAwaitDispatchEnvelope dispatch = new KafkaAwaitDispatchEnvelope(
        "tenant-1",
        "exec-1",
        "interaction-1",
        "corr-1",
        "AwaitPaymentProvider",
        System.currentTimeMillis() + 60_000L,
        PaymentRecord.class.getName(),
        PaymentStatus.class.getName(),
        "resume-token",
        paymentRecord,
        Map.of("topic", "csv-payments.payment.requests"));
    return PipelineJson.mapper().writeValueAsString(dispatch);
  }

  private static Message<String> failingMessage(String payload) {
    return Message.of(
        payload,
        () -> CompletableFuture.completedFuture(null),
        failure -> CompletableFuture.failedFuture(failure));
  }
}
