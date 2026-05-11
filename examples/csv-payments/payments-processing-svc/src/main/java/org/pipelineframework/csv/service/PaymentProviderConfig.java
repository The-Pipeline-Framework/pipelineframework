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

  /**
   * Simulated wait for the polling of the 3rd party service
   */
  @WithDefault("500")
  double waitMilliseconds();

  /**
   * Deterministic fraction of send-payment requests that should simulate a timeout.
   */
  @WithDefault("0.0")
  double sendTimeoutProbability();

  /**
   * Deterministic fraction of poll requests that should simulate a timeout.
   */
  @WithDefault("0.0")
  double pollTimeoutProbability();

  /**
   * Deterministic fraction of poll requests that should return a business rejection.
   */
  @WithDefault("0.0")
  double pollRejectProbability();
}
