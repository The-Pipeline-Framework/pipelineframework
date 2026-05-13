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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PaymentProviderConfigTest {

    private PaymentProviderConfig config;

    @BeforeEach
    void setUp() {
        config =
                new PaymentProviderConfig() {
                    @Override
                    public double permitsPerSecond() {
                        return 100.0;
                    }

                    @Override
                    public long timeoutMillis() {
                        return 5000L;
                    }

                    @Override
                    public double waitMilliseconds() {
                        return 100.0;
                    }

                    @Override
                    public double sendTimeoutProbability() {
                        return 0.0;
                    }

                    @Override
                    public double pollTimeoutProbability() {
                        return 0.0;
                    }

                    @Override
                    public double pollRejectProbability() {
                        return 0.0;
                    }
                };
    }

    @Test
    void testDefaultPermitsPerSecond() {
        assertThat(config.permitsPerSecond()).isEqualTo(100.0);
    }

    @Test
    void testDefaultTimeoutMillis() {
        assertThat(config.timeoutMillis()).isEqualTo(5000L);
    }

    @Test
    void testDefaultWaitMilliseconds() {
        assertThat(config.waitMilliseconds()).isEqualTo(100.0);
    }

    @Test
    void testDefaultSendTimeoutProbability() {
        assertThat(config.sendTimeoutProbability()).isEqualTo(0.0);
    }

    @Test
    void testDefaultPollTimeoutProbability() {
        assertThat(config.pollTimeoutProbability()).isEqualTo(0.0);
    }

    @Test
    void testDefaultPollRejectProbability() {
        assertThat(config.pollRejectProbability()).isEqualTo(0.0);
    }
}
