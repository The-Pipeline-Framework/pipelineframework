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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.eclipse.microprofile.config.spi.ConfigSource;
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
    void testDefaultProviderTimeoutProbability() {
        assertThat(config.providerTimeoutProbability()).isEqualTo(0.0);
    }

    @Test
    void testDefaultProviderRejectProbability() {
        assertThat(config.providerRejectProbability()).isEqualTo(0.0);
    }

    @Test
    void testDefaultResponseDelayMillis() {
        assertThat(config.responseDelayMillis()).isZero();
    }

    @Test
    void testDefaultCompletionBurstSize() {
        assertThat(defaultConfig().completionBurstSize()).isEqualTo(1);
    }

    @Test
    void testDefaultCompletionBurstFlushDelay() {
        assertThat(defaultConfig().completionBurstFlushDelay()).isEqualTo(Duration.ofSeconds(1));
    }

    private static PaymentProviderConfig defaultConfig() {
        Map<String, String> properties = Map.of();
        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder().withMapping(PaymentProviderConfig.class);
        builder.withSources(new ConfigSource() {
            @Override
            public Map<String, String> getProperties() {
                return properties;
            }

            @Override
            public Set<String> getPropertyNames() {
                return properties.keySet();
            }

            @Override
            public String getValue(String propertyName) {
                return properties.get(propertyName);
            }

            @Override
            public String getName() {
                return "test-config";
            }
        });
        SmallRyeConfig configured = builder.build();
        return configured.getConfigMapping(PaymentProviderConfig.class);
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
}
