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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Currency;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.awaitable.sqs.SqsAwaitCompletionEnvelope;
import org.pipelineframework.awaitable.sqs.SqsAwaitDispatchEnvelope;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

class PaymentProviderSqsAwaitMockTest {

  private SqsClient client;
  private PaymentProviderServiceMock paymentProvider;
  private PaymentProviderSqsAwaitMock mockProvider;

  @BeforeEach
  void setUp() {
    client = mock(SqsClient.class);
    paymentProvider = mock(PaymentProviderServiceMock.class);
    mockProvider = new PaymentProviderSqsAwaitMock(
        paymentProvider,
        new FakePaymentProviderConfig(),
        client);
  }

  @Test
  void pollOnceProcessesRequestSendsCompletionAndDeletesRequest() throws Exception {
    PaymentRecord paymentRecord = validPaymentRecord();
    PaymentStatus status = validPaymentStatus(paymentRecord);
    when(paymentProvider.processPayment(any(PaymentRecord.class))).thenReturn(status);
    when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
        .messages(message("receipt-1", dispatchJson(paymentRecord)))
        .build());

    assertDoesNotThrow(() -> mockProvider.pollOnce(enabledConfig()));

    ArgumentCaptor<PaymentRecord> requestCaptor = ArgumentCaptor.forClass(PaymentRecord.class);
    verify(paymentProvider).processPayment(requestCaptor.capture());
    assertEquals(paymentRecord.getAmount(), requestCaptor.getValue().getAmount());
    assertEquals(paymentRecord.getRecipient(), requestCaptor.getValue().getRecipient());
    assertEquals(paymentRecord.getCurrency(), requestCaptor.getValue().getCurrency());

    verify(client).sendMessage(argThat((SendMessageRequest request) -> {
      try {
        SqsAwaitCompletionEnvelope completion = PipelineJson.mapper()
            .readValue(request.messageBody(), SqsAwaitCompletionEnvelope.class);
        return request.queueUrl().equals("http://sqs.local/responses")
            && completion.tenantId().equals("tenant-1")
            && completion.interactionId().equals("interaction-1")
            && completion.correlationId().equals("corr-1")
            && completion.resumeToken().equals("resume-token")
            && completion.idempotencyKey().equals("interaction-1")
            && completion.actor().equals("csv-payments-sqs-mock-provider");
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }));
    verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
        request.queueUrl().equals("http://sqs.local/requests")
            && request.receiptHandle().equals("receipt-1")));
  }

  @Test
  void pollOnceLeavesRequestWhenProviderFails() throws Exception {
    PaymentRecord paymentRecord = validPaymentRecord();
    when(paymentProvider.processPayment(any(PaymentRecord.class))).thenThrow(new IllegalStateException("provider down"));
    when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
        .messages(message("receipt-2", dispatchJson(paymentRecord)))
        .build());

    assertDoesNotThrow(() -> mockProvider.pollOnce(enabledConfig()));

    verify(client, never()).sendMessage(any(SendMessageRequest.class));
    verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  void pollOnceLeavesRequestWhenResponseSendFails() throws Exception {
    PaymentRecord paymentRecord = validPaymentRecord();
    when(paymentProvider.processPayment(any(PaymentRecord.class))).thenReturn(validPaymentStatus(paymentRecord));
    when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
        .messages(message("receipt-3", dispatchJson(paymentRecord)))
        .build());
    when(client.sendMessage(any(SendMessageRequest.class))).thenThrow(new IllegalStateException("sqs down"));

    assertDoesNotThrow(() -> mockProvider.pollOnce(enabledConfig()));

    verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  void pollOnceKeepsMalformedRequestForQueueRedrive() {
    when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
        .messages(message("receipt-4", "{not-json"))
        .build());

    assertDoesNotThrow(() -> mockProvider.pollOnce(enabledConfig()));

    verify(paymentProvider, never()).processPayment(any(PaymentRecord.class));
    verify(client, never()).sendMessage(any(SendMessageRequest.class));
    verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  void pollOnceSkipsWhenDisabled() {
    assertDoesNotThrow(() -> mockProvider.pollOnce(config(false)));

    verify(client, never()).receiveMessage(any(ReceiveMessageRequest.class));
  }

  @Test
  void pollOnceRejectsVisibilityTimeoutAboveSqsLimit() {
    PaymentProviderSqsAwaitMock.SqsProviderConfig tooLarge = new PaymentProviderSqsAwaitMock.SqsProviderConfig(
        true,
        Optional.of("http://sqs.local/requests"),
        Optional.of("http://sqs.local/responses"),
        Optional.empty(),
        Optional.empty(),
        Duration.ZERO,
        Duration.ofSeconds(43_201),
        1,
        1);

    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> mockProvider.pollOnce(tooLarge));

    assertEquals("csv-payments.payment-provider.sqs.visibility-timeout must be between PT0S and PT43200S.",
        failure.getMessage());
    verify(client, never()).receiveMessage(any(ReceiveMessageRequest.class));
  }

  @Test
  void onStartupFailsFastWhenEnabledWithoutResponseQueue() {
    assertStartupFails(
        Optional.of("http://sqs.local/requests"),
        Optional.empty(),
        "csv-payments.payment-provider.sqs.response-queue-url must be configured when SQS provider is enabled.");
  }

  @Test
  void onStartupFailsFastWhenEnabledWithoutRequestQueue() {
    assertStartupFails(
        Optional.empty(),
        Optional.of("http://sqs.local/responses"),
        "csv-payments.payment-provider.sqs.request-queue-url must be configured when SQS provider is enabled.");
  }

  private void assertStartupFails(
      Optional<String> requestQueueUrl,
      Optional<String> responseQueueUrl,
      String expectedMessage) {
    System.setProperty("csv-payments.payment-provider.sqs.enabled", "true");
    setOrClear("csv-payments.payment-provider.sqs.request-queue-url", requestQueueUrl);
    setOrClear("csv-payments.payment-provider.sqs.response-queue-url", responseQueueUrl);
    try {
      IllegalStateException failure = assertThrows(IllegalStateException.class, () -> mockProvider.onStartup(null));
      assertEquals(expectedMessage, failure.getMessage());
    } finally {
      System.clearProperty("csv-payments.payment-provider.sqs.enabled");
      System.clearProperty("csv-payments.payment-provider.sqs.request-queue-url");
      System.clearProperty("csv-payments.payment-provider.sqs.response-queue-url");
    }
  }

  private static void setOrClear(String key, Optional<String> value) {
    if (value.isPresent()) {
      System.setProperty(key, value.get());
    } else {
      System.clearProperty(key);
    }
  }

  private static PaymentProviderSqsAwaitMock.SqsProviderConfig enabledConfig() {
    return config(true);
  }

  private static PaymentProviderSqsAwaitMock.SqsProviderConfig config(boolean enabled) {
    return new PaymentProviderSqsAwaitMock.SqsProviderConfig(
        enabled,
        Optional.of("http://sqs.local/requests"),
        Optional.of("http://sqs.local/responses"),
        Optional.empty(),
        Optional.empty(),
        Duration.ZERO,
        Duration.ofSeconds(30),
        1,
        1);
  }

  private static Message message(String receiptHandle, String body) {
    return Message.builder()
        .messageId(receiptHandle)
        .receiptHandle(receiptHandle)
        .body(body)
        .build();
  }

  private static String dispatchJson(PaymentRecord paymentRecord) throws Exception {
    SqsAwaitDispatchEnvelope dispatch = new SqsAwaitDispatchEnvelope(
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
        Map.of("queue", "requests"));
    return PipelineJson.mapper().writeValueAsString(dispatch);
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
    return new PaymentStatus()
        .setReference("provider-ref")
        .setStatus("Completed")
        .setMessage("settled")
        .setFee(new BigDecimal("0.12"))
        .setConversationId(UUID.randomUUID())
        .setStatusCode(1000L)
        .setPaymentRecord(paymentRecord)
        .setPaymentRecordId(paymentRecord.getId());
  }

  private static final class FakePaymentProviderConfig implements PaymentProviderConfig {
    @Override
    public double permitsPerSecond() {
      return 1000.0;
    }

    @Override
    public long timeoutMillis() {
      return 5000;
    }

    @Override
    public double providerTimeoutProbability() {
      return 0.0;
    }

    @Override
    public double providerRejectProbability() {
      return 0.0;
    }

    @Override
    public long responseDelayMillis() {
      return 0L;
    }

    @Override
    public Sqs sqs() {
      return new Sqs() {
        @Override
        public boolean enabled() {
          return false;
        }

        @Override
        public Optional<String> requestQueueUrl() {
          return Optional.empty();
        }

        @Override
        public Optional<String> responseQueueUrl() {
          return Optional.empty();
        }

        @Override
        public Optional<String> region() {
          return Optional.empty();
        }

        @Override
        public Optional<String> endpointOverride() {
          return Optional.empty();
        }

        @Override
        public Duration pollStartDelay() {
          return Duration.ZERO;
        }

        @Override
        public Duration visibilityTimeout() {
          return Duration.ofSeconds(30);
        }

        @Override
        public int waitTimeSeconds() {
          return 1;
        }

        @Override
        public int maxMessages() {
          return 1;
        }
      };
    }
  }
}
