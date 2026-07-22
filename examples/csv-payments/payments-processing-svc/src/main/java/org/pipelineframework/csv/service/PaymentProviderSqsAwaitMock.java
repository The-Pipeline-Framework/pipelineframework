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

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.sqs.SqsAwaitCompletionEnvelope;
import org.pipelineframework.awaitable.sqs.SqsAwaitDispatchEnvelope;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.csv.domain.PaymentRecord;
import org.pipelineframework.csv.domain.PaymentStatus;
import org.pipelineframework.csv.domain.PipelineDomainProtoAdapters;
import org.pipelineframework.csv.grpc.PipelineTypes;
import com.google.protobuf.util.JsonFormat;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * External mock provider for the CSV await/SQS self-host example.
 */
@ApplicationScoped
public class PaymentProviderSqsAwaitMock {

  private static final Logger LOG = Logger.getLogger(PaymentProviderSqsAwaitMock.class);
  private static final Duration DEFAULT_POLL_START_DELAY = Duration.ZERO;
  private static final Duration DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration EXECUTOR_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration INITIAL_FAILURE_BACKOFF = Duration.ofSeconds(1);
  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(30);

  @Inject
  PaymentProviderServiceMock paymentProvider;

  @Inject
  PaymentProviderConfig paymentProviderConfig;

  private volatile SqsClient client;
  private volatile ExecutorService pollExecutor;
  private volatile Future<?> pollFuture;
  private volatile boolean running;
  private final AtomicInteger consecutivePollFailures = new AtomicInteger();

  public PaymentProviderSqsAwaitMock() {
  }

  PaymentProviderSqsAwaitMock(
      PaymentProviderServiceMock paymentProvider,
      PaymentProviderConfig paymentProviderConfig,
      SqsClient client) {
    this.paymentProvider = paymentProvider;
    this.paymentProviderConfig = paymentProviderConfig;
    this.client = client;
  }

  void onStartup(@Observes StartupEvent event) {
    SqsProviderConfig config = SqsProviderConfig.fromRuntime();
    if (!config.enabled()) {
      return;
    }
    String requestQueueUrl = requiredQueueUrl(config.requestQueueUrl(),
        "csv-payments.payment-provider.sqs.request-queue-url");
    String responseQueueUrl = requiredQueueUrl(config.responseQueueUrl(),
        "csv-payments.payment-provider.sqs.response-queue-url");
    ensureExecutor();
    running = true;
    pollFuture = pollExecutor.submit(() -> {
      sleep(config.pollStartDelay());
      pollLoop();
    });
    LOG.infof("CSV SQS await mock provider enabled requestQueueUrl=%s responseQueueUrl=%s",
        requestQueueUrl,
        responseQueueUrl);
  }

  @PreDestroy
  void shutdown() {
    running = false;
    Future<?> activePollFuture = pollFuture;
    if (activePollFuture != null) {
      activePollFuture.cancel(true);
    }
    shutdownExecutor(pollExecutor);
    pollExecutor = null;
    SqsClient activeClient = client;
    if (activeClient == null) {
      return;
    }
    try {
      activeClient.close();
    } catch (Exception e) {
      LOG.debug("Failed closing SQS client for CSV await mock provider.", e);
    } finally {
      client = null;
    }
  }

  boolean pollOnce(SqsProviderConfig config) {
    if (!config.enabled()) {
      return true;
    }
    String requestQueueUrl = config.requestQueueUrl()
        .filter(url -> !url.isBlank())
        .orElseThrow(() -> new IllegalStateException(
            "csv-payments.payment-provider.sqs.request-queue-url must be configured when SQS provider is enabled."));
    int visibilityTimeoutSeconds = visibilityTimeoutSeconds(config);
    List<Message> messages;
    try {
      messages = sqsClient(config).receiveMessage(ReceiveMessageRequest.builder()
          .queueUrl(requestQueueUrl)
          .maxNumberOfMessages(config.maxMessages())
          .waitTimeSeconds(config.waitTimeSeconds())
          .visibilityTimeout(visibilityTimeoutSeconds)
          .build()).messages();
    } catch (RuntimeException e) {
      LOG.errorf(e, "Failed receiving CSV SQS await requests from queueUrl=%s", requestQueueUrl);
      sleepFailureBackoff();
      return false;
    }
    for (Message message : messages) {
      handleMessage(requestQueueUrl, message, config);
    }
    return true;
  }

  private static int visibilityTimeoutSeconds(SqsProviderConfig config) {
    long seconds = config.visibilityTimeout().toSeconds();
    if (seconds < 0 || seconds > 43_200) {
      throw new IllegalArgumentException(
          "csv-payments.payment-provider.sqs.visibility-timeout must be between PT0S and PT43200S.");
    }
    return (int) seconds;
  }

  private static String requiredQueueUrl(Optional<String> value, String configKey) {
    return value
        .filter(url -> !url.isBlank())
        .orElseThrow(() -> new IllegalStateException(configKey + " must be configured when SQS provider is enabled."));
  }

  private void pollLoop() {
    while (running && !Thread.currentThread().isInterrupted()) {
      try {
        if (pollOnce(SqsProviderConfig.fromRuntime())) {
          consecutivePollFailures.set(0);
        }
      } catch (Exception e) {
        LOG.error("CSV SQS await provider poll failed.", e);
        sleepFailureBackoff();
      }
    }
  }

  private void handleMessage(String requestQueueUrl, Message message, SqsProviderConfig config) {
    if (message == null || message.receiptHandle() == null) {
      return;
    }
    if (message.body() == null) {
      LOG.warnf("Leaving CSV SQS await request with null body for queue redrive id=%s", message.messageId());
      return;
    }
    SqsAwaitCompletionEnvelope completion;
    try {
      completion = handle(PipelineJson.mapper().readValue(message.body(), SqsAwaitDispatchEnvelope.class));
    } catch (Exception e) {
      LOG.errorf(e, "Failed processing CSV SQS await request id=%s", message.messageId());
      return;
    }
    try {
      delayCompletion();
      sqsClient(config).sendMessage(SendMessageRequest.builder()
          .queueUrl(config.responseQueueUrl()
              .filter(url -> !url.isBlank())
              .orElseThrow(() -> new IllegalStateException(
                  "csv-payments.payment-provider.sqs.response-queue-url must be configured when SQS provider is enabled.")))
          .messageBody(serialize(completion))
          .build());
      deleteMessage(requestQueueUrl, message.receiptHandle(), config);
    } catch (RuntimeException e) {
      LOG.errorf(e, "Failed sending CSV SQS await completion id=%s", message.messageId());
    }
  }

  private SqsAwaitCompletionEnvelope handle(SqsAwaitDispatchEnvelope dispatch) {
    PaymentRecord paymentRecord = PipelineDomainProtoAdapters.fromProto(paymentRecord(dispatch.requestPayload()));
    validatePaymentRecord(paymentRecord);
    PaymentStatus status = paymentProvider.processPayment(paymentRecord);
    return new SqsAwaitCompletionEnvelope(
        dispatch.tenantId(),
        dispatch.interactionId(),
        dispatch.correlationId(),
        dispatch.resumeToken(),
        dispatch.interactionId(),
        PipelineDomainProtoAdapters.toProto(status),
        "csv-payments-sqs-mock-provider");
  }

  private void delayCompletion() {
    long delayMillis = paymentProviderConfig.responseDelayMillis();
    if (delayMillis <= 0) {
      return;
    }
    sleep(Duration.ofMillis(delayMillis));
  }

  private void deleteMessage(String queueUrl, String receiptHandle, SqsProviderConfig config) {
    sqsClient(config).deleteMessage(DeleteMessageRequest.builder()
        .queueUrl(queueUrl)
        .receiptHandle(receiptHandle)
        .build());
  }

  private SqsClient sqsClient(SqsProviderConfig config) {
    SqsClient active = client;
    if (active != null) {
      return active;
    }
    synchronized (this) {
      if (client == null) {
        var builder = SqsClient.builder();
        builder.httpClientBuilder(UrlConnectionHttpClient.builder());
        config.region()
            .filter(region -> !region.isBlank())
            .ifPresent(region -> builder.region(Region.of(region)));
        config.endpointOverride()
            .filter(endpoint -> !endpoint.isBlank())
            .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
        client = builder.build();
      }
      return client;
    }
  }

  private void ensureExecutor() {
    if (pollExecutor == null) {
      pollExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "csv-sqs-await-provider");
        thread.setDaemon(true);
        return thread;
      });
    }
  }

  private void sleepFailureBackoff() {
    int failures = consecutivePollFailures.incrementAndGet();
    long delayMillis = Math.min(
        INITIAL_FAILURE_BACKOFF.toMillis() * (1L << Math.min(10, Math.max(0, failures - 1))),
        MAX_FAILURE_BACKOFF.toMillis());
    sleep(Duration.ofMillis(delayMillis));
  }

  private static void shutdownExecutor(ExecutorService executor) {
    if (executor == null) {
      return;
    }
    executor.shutdownNow();
    try {
      if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
        LOG.warnf("CSV SQS await provider executor did not terminate within %s", EXECUTOR_SHUTDOWN_TIMEOUT);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warnf(e, "Interrupted while shutting down CSV SQS await provider executor");
    }
  }

  private static void validatePaymentRecord(PaymentRecord paymentRecord) {
    if (paymentRecord == null) {
      throw new IllegalArgumentException("SQS await payment request payload must contain a PaymentRecord");
    }
    if (paymentRecord.amount() == null || paymentRecord.recipient() == null || paymentRecord.recipient().isBlank()
        || paymentRecord.currency() == null || paymentRecord.id() == null) {
      throw new IllegalArgumentException("PaymentRecord must include id, amount, recipient, and currency");
    }
  }

  private static PipelineTypes.PaymentRecord paymentRecord(Object payload) {
    try {
      var builder = PipelineTypes.PaymentRecord.newBuilder();
      JsonFormat.parser().ignoringUnknownFields().merge(PipelineJson.mapper().writeValueAsString(payload), builder);
      return builder.build();
    } catch (Exception e) {
      throw new IllegalArgumentException("SQS await payment request payload must contain a v3 PaymentRecord", e);
    }
  }

  private static String serialize(SqsAwaitCompletionEnvelope envelope) {
    try {
      return PipelineJson.mapper().writeValueAsString(envelope);
    } catch (Exception e) {
      throw new IllegalStateException("Failed serializing SQS await completion envelope", e);
    }
  }

  private static void sleep(Duration duration) {
    if (duration == null || duration.isZero() || duration.isNegative()) {
      return;
    }
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  record SqsProviderConfig(
      boolean enabled,
      Optional<String> requestQueueUrl,
      Optional<String> responseQueueUrl,
      Optional<String> region,
      Optional<String> endpointOverride,
      Duration pollStartDelay,
      Duration visibilityTimeout,
      int waitTimeSeconds,
      int maxMessages
  ) {
    static SqsProviderConfig fromRuntime() {
      var config = ConfigProvider.getConfig();
      return new SqsProviderConfig(
          config.getOptionalValue("csv-payments.payment-provider.sqs.enabled", Boolean.class).orElse(false),
          config.getOptionalValue("csv-payments.payment-provider.sqs.request-queue-url", String.class),
          config.getOptionalValue("csv-payments.payment-provider.sqs.response-queue-url", String.class),
          config.getOptionalValue("csv-payments.payment-provider.sqs.region", String.class),
          config.getOptionalValue("csv-payments.payment-provider.sqs.endpoint-override", String.class),
          config.getOptionalValue("csv-payments.payment-provider.sqs.poll-start-delay", Duration.class)
              .orElse(DEFAULT_POLL_START_DELAY),
          config.getOptionalValue("csv-payments.payment-provider.sqs.visibility-timeout", Duration.class)
              .orElse(DEFAULT_VISIBILITY_TIMEOUT),
          Math.max(1, Math.min(20,
              config.getOptionalValue("csv-payments.payment-provider.sqs.wait-time-seconds", Integer.class).orElse(1))),
          Math.max(1, Math.min(10,
              config.getOptionalValue("csv-payments.payment-provider.sqs.max-messages", Integer.class).orElse(1))));
    }
  }
}
