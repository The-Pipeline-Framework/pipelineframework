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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.kafka.KafkaAwaitCompletionEnvelope;
import org.pipelineframework.awaitable.kafka.KafkaAwaitDispatchEnvelope;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;

/**
 * External mock provider for the CSV await/Kafka example.
 */
@ApplicationScoped
@IfBuildProperty(name = "csv-payments.payment-provider.kafka.enabled", stringValue = "true", enableIfMissing = true)
public class PaymentProviderKafkaAwaitMock {

  static final String REQUEST_CHANNEL = "csv-payment-provider-requests";
  static final String RESULT_CHANNEL = "csv-payment-provider-results";
  private static final Logger LOG = Logger.getLogger(PaymentProviderKafkaAwaitMock.class);

  @Inject
  PaymentProviderServiceMock paymentProvider;

  @Inject
  PaymentProviderConfig paymentProviderConfig;

  @Inject
  @Channel(RESULT_CHANNEL)
  MutinyEmitter<String> results;

  @Incoming(REQUEST_CHANNEL)
  public CompletionStage<Void> consume(Message<String> message) {
    Objects.requireNonNull(message, "message must not be null");
    return Uni.createFrom().item(() -> parseDispatch(message.getPayload()))
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onItem().transform(this::handle)
        .onItem().transformToUni(this::delayCompletion)
        .onItem().transformToUni(completion -> results.send(serialize(completion)))
        .replaceWithVoid()
        .subscribeAsCompletionStage()
        .thenCompose(ignored -> message.ack())
        .exceptionallyCompose(failure -> {
          LOG.error("Failed processing Kafka await payment request", failure);
          return message.nack(failure);
        });
  }

  private KafkaAwaitCompletionEnvelope handle(KafkaAwaitDispatchEnvelope dispatch) {
    PaymentRecord paymentRecord = PipelineJson.mapper().convertValue(dispatch.requestPayload(), PaymentRecord.class);
    validatePaymentRecord(paymentRecord);
    PaymentStatus status = paymentProvider.processPayment(paymentRecord);
    return new KafkaAwaitCompletionEnvelope(
        dispatch.tenantId(),
        dispatch.interactionId(),
        dispatch.correlationId(),
        dispatch.resumeToken(),
        dispatch.interactionId(),
        status,
        "csv-payments-mock-provider");
  }

  private Uni<KafkaAwaitCompletionEnvelope> delayCompletion(KafkaAwaitCompletionEnvelope completion) {
    long delayMillis = paymentProviderConfig.responseDelayMillis();
    Uni<KafkaAwaitCompletionEnvelope> completionUni = Uni.createFrom().item(completion);
    if (delayMillis <= 0) {
      return completionUni;
    }
    return completionUni.onItem().delayIt().by(Duration.ofMillis(delayMillis));
  }

  private static void validatePaymentRecord(PaymentRecord paymentRecord) {
    if (paymentRecord == null) {
      throw new IllegalArgumentException("Kafka await payment request payload must contain a PaymentRecord");
    }
    if (paymentRecord.getAmount() == null || paymentRecord.getRecipient() == null || paymentRecord.getRecipient().isBlank()
        || paymentRecord.getCurrency() == null || paymentRecord.getId() == null) {
      throw new IllegalArgumentException("PaymentRecord must include id, amount, recipient, and currency");
    }
  }

  private static KafkaAwaitDispatchEnvelope parseDispatch(String payload) {
    try {
      return PipelineJson.mapper().readValue(payload, KafkaAwaitDispatchEnvelope.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid Kafka await dispatch envelope", e);
    }
  }

  private static String serialize(KafkaAwaitCompletionEnvelope envelope) {
    try {
      return PipelineJson.mapper().writeValueAsString(envelope);
    } catch (Exception e) {
      throw new IllegalStateException("Failed serializing Kafka await completion envelope", e);
    }
  }
}
