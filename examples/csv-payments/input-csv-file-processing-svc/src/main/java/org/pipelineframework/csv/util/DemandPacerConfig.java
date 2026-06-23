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

package org.pipelineframework.csv.util;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "csv-payments.reader-demand-pacer")
@Deprecated(since = "26.6.2", forRemoval = true)
public interface DemandPacerConfig {
  /**
   * Number of rows permitted in each rate-limiting period.
   *
   * @return the maximum number of rows allowed per period
   */
  @WithDefault("10")
  long rowsPerPeriod();

  /**
   * Rate-limiting period for demand pacing, expressed in milliseconds.
   *
   * @return the period length in milliseconds
   */
  @WithDefault("100")
  long millisPeriod();
}
