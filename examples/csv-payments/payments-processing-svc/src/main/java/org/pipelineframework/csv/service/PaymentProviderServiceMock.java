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
import org.pipelineframework.csv.common.dto.AckPaymentSentDto;
import org.pipelineframework.csv.common.dto.PaymentStatusDto;
import org.pipelineframework.csv.common.mapper.AckPaymentSentMapper;
import org.pipelineframework.csv.common.mapper.PaymentRecordMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;

@SuppressWarnings("UnstableApiUsage")
@ApplicationScoped
public class PaymentProviderServiceMock implements PaymentProviderService {
    
  private static final Logger LOG = Logger.getLogger(PaymentProviderServiceMock.class);

  private final RateLimiter rateLimiter;
  private final long timeoutMillis;
  private final AckPaymentSentMapper ackPaymentSentMapper = AckPaymentSentMapper.INSTANCE;
  private final PaymentRecordMapper paymentRecordMapper = PaymentRecordMapper.INSTANCE;
  private final PaymentStatusMapper paymentStatusMapper = PaymentStatusMapper.INSTANCE;

  @Inject
  public PaymentProviderServiceMock(PaymentProviderConfig config) {
    rateLimiter = RateLimiter.create(config.permitsPerSecond());
    timeoutMillis = config.timeoutMillis();

    LOG.infof(
        "PaymentProviderServiceMock initialized: permitsPerSecond=%s, timeoutMillis=%s",
        config.permitsPerSecond(),
        config.timeoutMillis());
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
    
    LOG.debug("Rate limiter permit acquired successfully");

    return ackPaymentSentMapper.fromDto(
        AckPaymentSentDto.builder()
            .id(UUID.randomUUID())
            .status(1000L)
            .message("OK but this is only a test")
            .conversationId(UUID.randomUUID())
            .paymentRecordId(requestMap.getPaymentRecordId())
            .paymentRecord(paymentRecordMapper.toDto(requestMap.getPaymentRecord()))
            .build());
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
    
    LOG.debug("Rate limiter permit acquired successfully");

    return paymentStatusMapper.fromDto(
        PaymentStatusDto.builder()
            .id(UUID.randomUUID())
            .reference("101")
            .status("Complete")
            .fee(new BigDecimal("1.01"))
            .message("Mock response")
            .ackPaymentSent(ackPaymentSentMapper.toDto(ackPaymentSent))
            .ackPaymentSentId(ackPaymentSent.getId())
            .build());
  }
}
