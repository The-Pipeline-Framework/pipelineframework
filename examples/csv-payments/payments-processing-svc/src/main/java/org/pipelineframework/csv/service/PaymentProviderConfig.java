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
import java.util.Optional;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "csv-payments.payment-provider")
public interface PaymentProviderConfig {
  /**
   * Rate-limiting of the 3rd party service (in seconds)
   */
  @WithDefault("1000.0")
  double permitsPerSecond();

  /**
   * Timeout of the 3rd party service
   */
  @WithDefault("5000")
  long timeoutMillis();

  /** Deterministic fraction of provider requests that should simulate a timeout. */
  @WithDefault("0.0")
  double providerTimeoutProbability();

  /** Deterministic fraction of provider requests that should return a business rejection. */
  @WithDefault("0.0")
  double providerRejectProbability();

  /** Optional delay before publishing await completions. */
  @WithDefault("0")
  long responseDelayMillis();

  /** Number of completions held before the mock provider publishes a burst. */
  @WithDefault("1")
  int completionBurstSize();

  /** Maximum time the mock provider holds a final partial completion burst. */
  @WithDefault("PT1S")
  Duration completionBurstFlushDelay();

  /** SQS await mock provider configuration. */
  Sqs sqs();

  /** SQS await mock provider settings for the container self-host reference. */
  interface Sqs {

    /** Whether the SQS await mock provider poller starts with the application. */
    @WithDefault("false")
    boolean enabled();

    /** SQS queue URL that receives payment-provider await requests. */
    Optional<String> requestQueueUrl();

    /** SQS queue URL that receives payment-provider await completion responses. */
    Optional<String> responseQueueUrl();

    /** AWS region used by the SQS client. */
    Optional<String> region();

    /** Optional SQS endpoint override, used by LocalStack in the self-host reference. */
    Optional<String> endpointOverride();

    /** Delay before the provider starts polling the request queue. */
    @WithDefault("PT0S")
    Duration pollStartDelay();

    /** SQS visibility timeout used while processing provider requests. */
    @WithDefault("PT30S")
    Duration visibilityTimeout();

    /** Long-poll wait time in seconds for request queue receives. */
    @WithDefault("1")
    int waitTimeSeconds();

    /** Maximum number of request messages to receive per poll. */
    @WithDefault("1")
    int maxMessages();
  }
}
