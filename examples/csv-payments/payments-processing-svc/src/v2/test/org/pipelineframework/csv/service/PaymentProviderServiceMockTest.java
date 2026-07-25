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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Currency;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus;

class PaymentProviderServiceMockTest {

  @Test
  @DisplayName("Should process one payment record and return PaymentStatus")
  void processPayment_happyPath_shouldReturnPaymentStatus() {
    PaymentRecord paymentRecord = testPaymentRecord();
    PaymentProviderServiceMock paymentProvider = new PaymentProviderServiceMock(new FakePaymentProviderConfig());

    PaymentStatus paymentStatus = paymentProvider.processPayment(paymentRecord);

    assertThat(paymentStatus).isNotNull();
    assertThat(paymentStatus).isInstanceOf(ApprovedPaymentStatus.class);
    assertThat(paymentStatus.getReference()).isEqualTo("101");
    assertThat(paymentStatus.getStatus()).isEqualTo("Complete");
    assertThat(paymentStatus.getFee()).isEqualTo(new BigDecimal("1.01"));
    assertThat(paymentStatus.getMessage()).isEqualTo("Mock response");
    assertThat(paymentStatus.getConversationId()).isNotNull();
    assertThat(paymentStatus.getStatusCode()).isEqualTo(1000L);
    assertThat(paymentStatus.getPaymentRecord()).isEqualTo(paymentRecord);
    assertThat(paymentStatus.getPaymentRecordId()).isEqualTo(paymentRecord.getId());
  }

  @Test
  @DisplayName("Should throw StatusRuntimeException when provider is throttled")
  void processPayment_throttled_shouldThrowException() {
    PaymentProviderServiceMock paymentProvider = new PaymentProviderServiceMock(new NegativeTimeoutPaymentProviderConfig());

    StatusRuntimeException thrown =
        assertThrows(StatusRuntimeException.class, () -> paymentProvider.processPayment(testPaymentRecord()));

    assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(thrown.getStatus().getDescription())
        .isEqualTo("Payment service is currently throttled. Please try again later.");
  }

  @Test
  @DisplayName("Should deterministically simulate provider timeout")
  void processPayment_configuredTimeout_shouldThrowDeadlineExceeded() {
    PaymentProviderServiceMock paymentProvider = new PaymentProviderServiceMock(new TimeoutPaymentProviderConfig());

    StatusRuntimeException thrown =
        assertThrows(StatusRuntimeException.class, () -> paymentProvider.processPayment(testPaymentRecord()));

    assertThat(thrown.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
    assertThat(thrown.getStatus().getDescription())
        .isEqualTo("Mock payment provider timed out while processing payment.");
  }

  @Test
  @DisplayName("Should deterministically return rejected payment status")
  void processPayment_configuredReject_shouldReturnRejectedStatus() {
    PaymentProviderServiceMock paymentProvider = new PaymentProviderServiceMock(new RejectPaymentProviderConfig());

    PaymentStatus paymentStatus = paymentProvider.processPayment(testPaymentRecord());

    assertThat(paymentStatus).isInstanceOf(UnapprovedPaymentStatus.class);
    assertThat(paymentStatus.getStatus()).isEqualTo("Rejected");
    assertThat(paymentStatus.getMessage()).isEqualTo("Mock payment provider rejected the payment.");
    assertThat(paymentStatus.getPaymentRecordId()).isNotNull();
  }

  private static PaymentRecord testPaymentRecord() {
    PaymentRecord paymentRecord = new PaymentRecord()
        .setCsvId(UUID.randomUUID().toString())
        .setRecipient("John Doe")
        .setAmount(BigDecimal.valueOf(100.00))
        .setCurrency(Currency.getInstance("USD"));
    paymentRecord.setId(UUID.randomUUID());
    return paymentRecord;
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
    public int completionBurstSize() {
      return 1;
    }

    @Override
    public Duration completionBurstFlushDelay() {
      return Duration.ofSeconds(1);
    }

    @Override
    public Sqs sqs() {
      return disabledSqs();
    }
  }

  private static PaymentProviderConfig.Sqs disabledSqs() {
    return new PaymentProviderConfig.Sqs() {
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

  private static final class NegativeTimeoutPaymentProviderConfig extends FakePaymentProviderConfig {
    @Override
    public long timeoutMillis() {
      return -1L;
    }
  }

  private static final class TimeoutPaymentProviderConfig extends FakePaymentProviderConfig {
    @Override
    public double providerTimeoutProbability() {
      return 1.0;
    }
  }

  private static final class RejectPaymentProviderConfig extends FakePaymentProviderConfig {
    @Override
    public double providerRejectProbability() {
      return 1.0;
    }
  }
}
