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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitTelemetry;

class PaymentProviderKafkaAwaitMockTest {

  @Test
  void consumeFailureNacksWithoutAcknowledging() {
    AtomicBoolean acknowledged = new AtomicBoolean();
    AtomicBoolean nacked = new AtomicBoolean();
    Message<String> message = Message.of(
        "not-a-kafka-await-dispatch-envelope",
        () -> {
          acknowledged.set(true);
          return CompletableFuture.completedFuture(null);
        },
        failure -> {
          nacked.set(true);
          return CompletableFuture.completedFuture(null);
        });

    PaymentProviderKafkaAwaitMock provider = new PaymentProviderKafkaAwaitMock(AwaitTelemetry.disabled());

    provider.consume(message).toCompletableFuture().join();

    assertThat(nacked).isTrue();
    assertThat(acknowledged).isFalse();
  }
}
