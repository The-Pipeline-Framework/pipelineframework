/*
 * Copyright (c) 2026 Mariano Barcia
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

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PaymentProviderSqsAwaitMockTest {

  @Test
  void visibilityTimeoutCoversSerialProcessingForTheWholeReceivedBatch() {
    PaymentProviderConfig providerConfig = new PaymentProviderServiceMockTest.FakePaymentProviderConfig() {
      @Override
      public long timeoutMillis() {
        return 1_250L;
      }

      @Override
      public long responseDelayMillis() {
        return 500L;
      }
    };
    PaymentProviderSqsAwaitMock mock = new PaymentProviderSqsAwaitMock(null, providerConfig, null);
    PaymentProviderSqsAwaitMock.SqsProviderConfig sqsConfig =
        new PaymentProviderSqsAwaitMock.SqsProviderConfig(
            true,
            Optional.of("http://localhost:4566/queue/requests"),
            Optional.of("http://localhost:4566/queue/responses"),
            Optional.empty(),
            Optional.empty(),
            Duration.ZERO,
            Duration.ofSeconds(1),
            1,
            10);

    assertEquals(18, mock.visibilityTimeoutSeconds(sqsConfig));
  }
}
