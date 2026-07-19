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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class PaymentProviderCompletionProfileTest {

  @Test
  void releasesACompletionBurstWhenTheConfiguredThresholdIsReached() throws Exception {
    try (PaymentProviderCompletionProfile<String> profile = new PaymentProviderCompletionProfile<>(
        2,
        Duration.ofSeconds(1),
        "payment-provider-completion-profile-test")) {
      var first = profile.releaseWhenReady("first").toCompletableFuture();
      var second = profile.releaseWhenReady("second").toCompletableFuture();

      assertEquals("first", first.get(1, TimeUnit.SECONDS));
      assertEquals("second", second.get(1, TimeUnit.SECONDS));
      assertEquals(2, profile.observation().inFlight());
      assertEquals(2, profile.observation().maxInFlight());

      profile.completionHandled();
      profile.completionHandled();

      assertEquals(0, profile.observation().inFlight());
      assertEquals(2, profile.observation().maxInFlight());
    }
  }

  @Test
  void flushesTheFinalPartialBurstWithoutWaitingForAnotherRequest() throws Exception {
    try (PaymentProviderCompletionProfile<String> profile = new PaymentProviderCompletionProfile<>(
        3,
        Duration.ofMillis(100),
        "payment-provider-completion-profile-test")) {
      var first = profile.releaseWhenReady("first").toCompletableFuture();
      var second = profile.releaseWhenReady("second").toCompletableFuture();

      assertFalse(first.isDone());
      assertFalse(second.isDone());
      assertEquals("first", first.get(2, TimeUnit.SECONDS));
      assertEquals("second", second.get(2, TimeUnit.SECONDS));
      assertTrue(profile.observation().maxInFlight() >= 2);
    }
  }

  @Test
  void closeFlushesTheFinalPartialBurstImmediately() throws Exception {
    var profile = new PaymentProviderCompletionProfile<String>(
        3,
        Duration.ofMinutes(1),
        "payment-provider-completion-profile-test");
    var pending = profile.releaseWhenReady("pending").toCompletableFuture();

    assertFalse(pending.isDone());
    profile.close();

    assertEquals("pending", pending.get(1, TimeUnit.SECONDS));
  }

  `@Test`
  void staleFlushDoesNotReleaseTheNextBurstEarly() throws Exception {
    try (PaymentProviderCompletionProfile<String> profile = new PaymentProviderCompletionProfile<>(
        2,
        Duration.ofMillis(1000),
        "payment-provider-completion-profile-test")) {
      profile.releaseWhenReady("first");
      profile.releaseWhenReady("second").toCompletableFuture().get(1, TimeUnit.SECONDS);
      Thread.sleep(250);
      var third = profile.releaseWhenReady("third").toCompletableFuture();

      Thread.sleep(500);
      assertFalse(third.isDone());
      assertEquals("third", third.get(2, TimeUnit.SECONDS));
    }
  }
}
