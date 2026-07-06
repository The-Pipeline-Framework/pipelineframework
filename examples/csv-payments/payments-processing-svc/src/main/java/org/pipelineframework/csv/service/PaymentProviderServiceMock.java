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
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus;

@SuppressWarnings("UnstableApiUsage")
@ApplicationScoped
public class PaymentProviderServiceMock implements PaymentProviderService {
    
  private static final Logger LOG = Logger.getLogger(PaymentProviderServiceMock.class);

  private final RateLimiter rateLimiter;
  private final long timeoutMillis;
  private final double timeoutProbability;
  private final double rejectProbability;

  @Inject
  public PaymentProviderServiceMock(PaymentProviderConfig config) {
    rateLimiter = RateLimiter.create(config.permitsPerSecond());
    timeoutMillis = config.timeoutMillis();
    timeoutProbability = normalizeProbability(config.providerTimeoutProbability());
    rejectProbability = normalizeProbability(config.providerRejectProbability());

    LOG.infof(
        "PaymentProviderServiceMock initialized: permitsPerSecond=%s, timeoutMillis=%s, providerTimeoutProbability=%s, providerRejectProbability=%s",
        config.permitsPerSecond(),
        config.timeoutMillis(),
        timeoutProbability,
        rejectProbability);
  }

  @Override
  public PaymentStatus processPayment(@NonNull PaymentRecord paymentRecord) {
    LOG.debugf("processPayment called with paymentRecordId=%s, csvId=%s",
        paymentRecord.getId(), paymentRecord.getCsvId());
        
    LOG.debugf("Attempting to acquire rate limiter permit with timeout: %sms", timeoutMillis);
    boolean acquired = (this.timeoutMillis != -1L && rateLimiter.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS));
    
    if (!acquired) {
      LOG.debugf("Failed to acquire rate limiter permit within timeout period: %sms", timeoutMillis);
      throw new StatusRuntimeException(
          Status.RESOURCE_EXHAUSTED.withDescription(
              "Payment service is currently throttled. Please try again later."));
    }

    if (shouldSimulate(timeoutProbability, simulationKey(paymentRecord), "provider-timeout")) {
      LOG.warnf("Simulating payment-provider timeout for paymentRecordId=%s", paymentRecord.getId());
      throw new StatusRuntimeException(
          Status.DEADLINE_EXCEEDED.withDescription(
              "Mock payment provider timed out while processing payment."));
    }
    
    LOG.debug("Rate limiter permit acquired successfully");

    PaymentStatus paymentStatus =
        shouldSimulate(rejectProbability, simulationKey(paymentRecord), "provider-reject")
            ? new UnapprovedPaymentStatus()
            : new ApprovedPaymentStatus();
    paymentStatus.setId(UUID.randomUUID());
    paymentStatus.setReference("101");
    paymentStatus.setConversationId(UUID.randomUUID());
    paymentStatus.setStatusCode(1000L);
    if (paymentStatus instanceof UnapprovedPaymentStatus) {
      paymentStatus.setStatus("Rejected");
      paymentStatus.setMessage("Mock payment provider rejected the payment.");
    } else {
      paymentStatus.setStatus("Complete");
      paymentStatus.setMessage("Mock response");
    }
    paymentStatus.setFee(new BigDecimal("1.01"));
    paymentStatus.setPaymentRecord(paymentRecord);
    paymentStatus.setPaymentRecordId(
        paymentRecord.getId() != null
            ? paymentRecord.getId()
            : stableFallbackPaymentRecordId(paymentRecord));
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

  private static String simulationKey(PaymentRecord paymentRecord) {
    if (paymentRecord.getCsvId() != null) {
      return paymentRecord.getCsvId();
    }
    if (paymentRecord.getId() != null) {
      return paymentRecord.getId().toString();
    }
    return "payment-unknown";
  }

  private static UUID stableFallbackPaymentRecordId(PaymentRecord paymentRecord) {
    String stableSource = String.join("|",
        paymentRecord.getCsvId() == null ? "" : paymentRecord.getCsvId(),
        paymentRecord.getRecipient() == null ? "" : paymentRecord.getRecipient(),
        paymentRecord.getAmount() == null ? "" : paymentRecord.getAmount().toPlainString());
    return UUID.nameUUIDFromBytes(stableSource.getBytes(StandardCharsets.UTF_8));
  }
}
