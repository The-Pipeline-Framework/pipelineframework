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
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.google.common.util.concurrent.RateLimiter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.NonNull;
import org.jboss.logging.Logger;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.domain.SendPaymentRequest;

@SuppressWarnings("UnstableApiUsage")
@ApplicationScoped
public class PaymentProviderServiceMock implements PaymentProviderService {
    
  private static final Logger LOG = Logger.getLogger(PaymentProviderServiceMock.class);

  private final RateLimiter rateLimiter;
  private final long timeoutMillis;
  private final double sendTimeoutProbability;
  private final double pollTimeoutProbability;
  private final double pollRejectProbability;

  @Inject
  public PaymentProviderServiceMock(PaymentProviderConfig config) {
    rateLimiter = RateLimiter.create(config.permitsPerSecond());
    timeoutMillis = config.timeoutMillis();
    sendTimeoutProbability = normalizeProbability(config.sendTimeoutProbability());
    pollTimeoutProbability = normalizeProbability(config.pollTimeoutProbability());
    pollRejectProbability = normalizeProbability(config.pollRejectProbability());

    LOG.infof(
        "PaymentProviderServiceMock initialized: permitsPerSecond=%s, timeoutMillis=%s, sendTimeoutProbability=%s, pollTimeoutProbability=%s, pollRejectProbability=%s",
        config.permitsPerSecond(),
        config.timeoutMillis(),
        sendTimeoutProbability,
        pollTimeoutProbability,
        pollRejectProbability);
  }

  @Override
  public AckPaymentSent sendPayment(
      @NonNull SendPaymentRequest requestMap) {
    LOG.debugf("sendPayment called with request: amount=%s, currency=%s, reference=%s, paymentRecordId=%s",
        requestMap.getAmount(), requestMap.getCurrency(), requestMap.getReference(), requestMap.getPaymentRecordId());
        
    // Try to acquire with timeout
    LOG.debugf("Attempting to acquire rate limiter permit with timeout: %sms", timeoutMillis);
    boolean acquired = (this.timeoutMillis != -1L && rateLimiter.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS));
    
    if (!acquired) {
      LOG.debugf("Failed to acquire rate limiter permit within timeout period: %sms", timeoutMillis);
      throw new StatusRuntimeException(
          Status.RESOURCE_EXHAUSTED.withDescription(
              "Payment service is currently throttled. Please try again later."));
    }

    if (shouldSimulate(sendTimeoutProbability, sendSimulationKey(requestMap), "send-timeout")) {
      LOG.warnf("Simulating send-payment timeout for paymentRecordId=%s", requestMap.getPaymentRecordId());
      throw new StatusRuntimeException(
          Status.DEADLINE_EXCEEDED.withDescription(
              "Mock payment provider timed out while sending payment."));
    }
    
    LOG.debug("Rate limiter permit acquired successfully");

    AckPaymentSent ackPaymentSent = new AckPaymentSent(UUID.randomUUID());
    ackPaymentSent.setStatus(1000L);
    ackPaymentSent.setMessage("OK but this is only a test");
    ackPaymentSent.setConversationId(UUID.randomUUID());
    ackPaymentSent.setPaymentRecordId(requestMap.getPaymentRecordId());
    ackPaymentSent.setPaymentRecord(requestMap.getPaymentRecord());
    return ackPaymentSent;
  }

  @Override
  public PaymentStatus getPaymentStatus(@NonNull AckPaymentSent ackPaymentSent) {
    LOG.debugf("getPaymentStatus called with AckPaymentSent: id=%s, conversationId=%s, paymentRecordId=%s", 
        ackPaymentSent.getId(), ackPaymentSent.getConversationId(), ackPaymentSent.getPaymentRecordId());
        
    // Try to acquire with timeout
    LOG.debugf("Attempting to acquire rate limiter permit with timeout: %sms", timeoutMillis);
    boolean acquired = (this.timeoutMillis != -1L && rateLimiter.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS));
    
    if (!acquired) {
      LOG.warnf("Failed to acquire rate limiter permit within timeout period: %sms", timeoutMillis);
      throw new StatusRuntimeException(
          Status.RESOURCE_EXHAUSTED.withDescription(
              "Failed to acquire permit within timeout period. The payment status service is currently throttled."));
    }

    if (shouldSimulate(pollTimeoutProbability, pollSimulationKey(ackPaymentSent), "poll-timeout")) {
      LOG.warnf("Simulating payment-status timeout for conversationId=%s", ackPaymentSent.getConversationId());
      throw new StatusRuntimeException(
          Status.DEADLINE_EXCEEDED.withDescription(
              "Mock payment provider timed out while polling payment status."));
    }
    
    LOG.debug("Rate limiter permit acquired successfully");

    PaymentStatus paymentStatus = new PaymentStatus();
    paymentStatus.setId(UUID.randomUUID());
    paymentStatus.setReference("101");
    if (shouldSimulate(pollRejectProbability, pollSimulationKey(ackPaymentSent), "poll-reject")) {
      paymentStatus.setStatus("Rejected");
      paymentStatus.setMessage("Mock payment provider rejected the payment.");
    } else {
      paymentStatus.setStatus("Complete");
      paymentStatus.setMessage("Mock response");
    }
    paymentStatus.setFee(new BigDecimal("1.01"));
    paymentStatus.setAckPaymentSent(ackPaymentSent);
    paymentStatus.setAckPaymentSentId(
        ackPaymentSent.getId() != null
            ? ackPaymentSent.getId()
            : UUID.nameUUIDFromBytes((pollSimulationKey(ackPaymentSent) + ":ack-id").getBytes(StandardCharsets.UTF_8)));
    paymentStatus.setPaymentRecord(ackPaymentSent.getPaymentRecord());
    paymentStatus.setPaymentRecordId(
        ackPaymentSent.getPaymentRecordId() != null
            ? ackPaymentSent.getPaymentRecordId()
            : stableFallbackPaymentRecordId(ackPaymentSent));
    return paymentStatus;
  }

  private static double normalizeProbability(double probability) {
    if (Double.isNaN(probability)) {
      return 0.0;
    }
    return Math.max(0.0, Math.min(1.0, probability));
  }

  private boolean shouldSimulate(double probability, String key, String salt) {
    if (probability <= 0.0) {
      return false;
    }
    if (probability >= 1.0) {
      return true;
    }
    long normalized = Integer.toUnsignedLong(java.util.Objects.hash(key, salt));
    long threshold = Math.round(probability * 10_000d);
    return normalized % 10_000L < threshold;
  }

  private static String sendSimulationKey(SendPaymentRequest request) {
    if (request.getPaymentRecord() != null && request.getPaymentRecord().getCsvId() != null) {
      return request.getPaymentRecord().getCsvId();
    }
    if (request.getPaymentRecordId() != null) {
      return request.getPaymentRecordId().toString();
    }
    return "send-unknown";
  }

  private static String pollSimulationKey(AckPaymentSent ackPaymentSent) {
    if (ackPaymentSent.getPaymentRecord() != null
        && ackPaymentSent.getPaymentRecord().getCsvId() != null) {
      return ackPaymentSent.getPaymentRecord().getCsvId();
    }
    if (ackPaymentSent.getPaymentRecordId() != null) {
      return ackPaymentSent.getPaymentRecordId().toString();
    }
    if (ackPaymentSent.getConversationId() != null) {
      return ackPaymentSent.getConversationId().toString();
    }
    return "poll-unknown";
  }

  private static UUID stableFallbackPaymentRecordId(AckPaymentSent ackPaymentSent) {
    String stableSource = String.join("|",
        ackPaymentSent.getId() == null ? "" : ackPaymentSent.getId().toString(),
        ackPaymentSent.getConversationId() == null ? "" : ackPaymentSent.getConversationId().toString(),
        ackPaymentSent.getPaymentRecord() == null || ackPaymentSent.getPaymentRecord().getCsvId() == null
            ? ""
            : ackPaymentSent.getPaymentRecord().getCsvId());
    return UUID.nameUUIDFromBytes(stableSource.getBytes(StandardCharsets.UTF_8));
  }
}
