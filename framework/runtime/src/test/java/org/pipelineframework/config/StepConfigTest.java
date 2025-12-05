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

package org.pipelineframework.config;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class StepConfigTest {

    @Test
    void testDefaultValues() {
        // Given
        StepConfig config = new StepConfig();

        // Then
        assertEquals(3, config.retryLimit());
        assertEquals(Duration.ofMillis(2000), config.retryWait());
        assertFalse(config.parallel());
        assertFalse(config.recoverOnFailure());
        assertEquals(Duration.ofSeconds(30), config.maxBackoff());
        assertFalse(config.jitter());
    }

    @Test
    void testConfigFromPipelineStepConfig() {
        // Given
        PipelineStepConfig.StepConfig mockStepConfig = new PipelineStepConfig.StepConfig() {
            @Override
            public Integer retryLimit() {
                return 5;
            }

            @Override
            public Long retryWaitMs() {
                return 3000L;
            }

            @Override
            public Boolean parallel() {
                return true;
            }

            @Override
            public Boolean recoverOnFailure() {
                return true;
            }

            @Override
            public Long maxBackoff() {
                return 60000L;
            }

            @Override
            public Boolean jitter() {
                return true;
            }

            @Override
            public Integer backpressureBufferCapacity() {
                return 2048;
            }

            @Override
            public String backpressureStrategy() {
                return "DROP";
            }

            @Override
            public Integer order() {
                return 100;
            }
        };

        // When
        StepConfig config = new StepConfig(mockStepConfig);

        // Then
        assertEquals(5, config.retryLimit());
        assertEquals(Duration.ofMillis(3000), config.retryWait());
        assertTrue(config.parallel());
        assertTrue(config.recoverOnFailure());
        assertEquals(Duration.ofSeconds(60), config.maxBackoff());
        assertTrue(config.jitter());
        assertEquals(2048, config.backpressureBufferCapacity());
        assertEquals("DROP", config.backpressureStrategy());
    }

    @Test
    void testRetryLimitSetter() {
        // Given
        StepConfig config = new StepConfig();

        // When
        StepConfig result = config.retryLimit(5);

        // Then
        assertEquals(5, config.retryLimit());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testRetryWaitSetter() {
        // Given
        StepConfig config = new StepConfig();
        Duration newDuration = Duration.ofSeconds(1);

        // When
        StepConfig result = config.retryWait(newDuration);

        // Then
        assertEquals(newDuration, config.retryWait());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testRetryWaitSetterRejectsNull() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(NullPointerException.class, () -> config.retryWait(null));
    }

    @Test
    void testParallelSetter() {
        // Given
        StepConfig config = new StepConfig();

        // When
        StepConfig result = config.parallel(true);

        // Then
        assertTrue(config.parallel());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testRecoverOnFailureSetter() {
        // Given
        StepConfig config = new StepConfig();

        // When
        StepConfig result = config.recoverOnFailure(true);

        // Then
        assertTrue(config.recoverOnFailure());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testRecoverOnFailureDefaultValue() {
        // Given
        StepConfig config = new StepConfig();

        // Then
        assertFalse(config.recoverOnFailure());
    }

    @Test
    void testRecoverOnFailureSetterFalse() {
        // Given
        StepConfig config = new StepConfig();
        config.recoverOnFailure(true); // Set to true first

        // When
        StepConfig result = config.recoverOnFailure(false);

        // Then
        assertFalse(config.recoverOnFailure());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testMaxBackoffSetter() {
        // Given
        StepConfig config = new StepConfig();
        Duration newDuration = Duration.ofMinutes(2);

        // When
        StepConfig result = config.maxBackoff(newDuration);

        // Then
        assertEquals(newDuration, config.maxBackoff());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testMaxBackoffSetterRejectsNull() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(NullPointerException.class, () -> config.maxBackoff(null));
    }

    @Test
    void testJitterSetter() {
        // Given
        StepConfig config = new StepConfig();

        // When
        StepConfig result = config.jitter(true);

        // Then
        assertTrue(config.jitter());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testToString() {
        // Given
        StepConfig config = new StepConfig();

        // When
        String result = config.toString();

        // Then
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(result.contains("StepConfig"));
    }

    @Test
    void testConfigFromPipelineStepConfigWithNullConfig() {
        // Given - null config should use defaults
        PipelineStepConfig.StepConfig nullConfig = null;

        // When
        StepConfig config = new StepConfig(nullConfig);

        // Then - should have default values
        assertEquals(3, config.retryLimit());
        assertEquals(Duration.ofMillis(2000), config.retryWait());
        assertFalse(config.parallel());
        assertFalse(config.recoverOnFailure());
        assertEquals(Duration.ofSeconds(30), config.maxBackoff());
        assertFalse(config.jitter());
        assertEquals(1024, config.backpressureBufferCapacity());
        assertEquals("BUFFER", config.backpressureStrategy());
    }

    @Test
    void testBackpressureBufferCapacitySetter() {
        // Given
        StepConfig config = new StepConfig();

        // When
        StepConfig result = config.backpressureBufferCapacity(2048);

        // Then
        assertEquals(2048, config.backpressureBufferCapacity());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testBackpressureBufferCapacitySetterRejectsZero() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.backpressureBufferCapacity(0));
    }

    @Test
    void testBackpressureBufferCapacitySetterRejectsNegative() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.backpressureBufferCapacity(-1));
    }

    @Test
    void testBackpressureStrategySetter() {
        // Given
        StepConfig config = new StepConfig();

        // When
        StepConfig result = config.backpressureStrategy("DROP");

        // Then
        assertEquals("DROP", config.backpressureStrategy());
        assertSame(config, result); // Fluent API
    }

    @Test
    void testBackpressureStrategySetterNormalizesCaseBuffer() {
        // Given
        StepConfig config = new StepConfig();

        // When
        config.backpressureStrategy("buffer");

        // Then
        assertEquals("BUFFER", config.backpressureStrategy());
    }

    @Test
    void testBackpressureStrategySetterNormalizesCaseDrop() {
        // Given
        StepConfig config = new StepConfig();

        // When
        config.backpressureStrategy("drop");

        // Then
        assertEquals("DROP", config.backpressureStrategy());
    }

    @Test
    void testBackpressureStrategySetterTrimsWhitespace() {
        // Given
        StepConfig config = new StepConfig();

        // When
        config.backpressureStrategy("  BUFFER  ");

        // Then
        assertEquals("BUFFER", config.backpressureStrategy());
    }

    @Test
    void testBackpressureStrategySetterRejectsNull() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(NullPointerException.class, () -> config.backpressureStrategy(null));
    }

    @Test
    void testBackpressureStrategySetterRejectsInvalidStrategy() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.backpressureStrategy("INVALID"));
    }

    @Test
    void testBackpressureStrategySetterRejectsEmptyString() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.backpressureStrategy(""));
    }

    @Test
    void testBackpressureStrategySetterRejectsWhitespaceOnly() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.backpressureStrategy("   "));
    }

    @Test
    void testRetryLimitSetterRejectsNegative() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.retryLimit(-1));
    }

    @Test
    void testRetryLimitSetterAcceptsZero() {
        // Given
        StepConfig config = new StepConfig();

        // When
        StepConfig result = config.retryLimit(0);

        // Then
        assertEquals(0, config.retryLimit());
        assertSame(config, result);
    }

    @Test
    void testRetryWaitSetterRejectsZeroDuration() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.retryWait(Duration.ZERO));
    }

    @Test
    void testRetryWaitSetterRejectsNegativeDuration() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(
                IllegalArgumentException.class, () -> config.retryWait(Duration.ofMillis(-100)));
    }

    @Test
    void testMaxBackoffSetterRejectsZeroDuration() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> config.maxBackoff(Duration.ZERO));
    }

    @Test
    void testMaxBackoffSetterRejectsNegativeDuration() {
        // Given
        StepConfig config = new StepConfig();

        // When/Then
        assertThrows(
                IllegalArgumentException.class, () -> config.maxBackoff(Duration.ofSeconds(-1)));
    }

    @Test
    void testDefaultBackpressureBufferCapacity() {
        // Given
        StepConfig config = new StepConfig();

        // Then
        assertEquals(1024, config.backpressureBufferCapacity());
    }

    @Test
    void testDefaultBackpressureStrategy() {
        // Given
        StepConfig config = new StepConfig();

        // Then
        assertEquals("BUFFER", config.backpressureStrategy());
    }

    @Test
    void testToStringContainsAllProperties() {
        // Given
        StepConfig config = new StepConfig()
                .retryLimit(5)
                .retryWait(Duration.ofSeconds(3))
                .parallel(true)
                .recoverOnFailure(true)
                .maxBackoff(Duration.ofMinutes(1))
                .jitter(true)
                .backpressureBufferCapacity(2048)
                .backpressureStrategy("DROP");

        // When
        String result = config.toString();

        // Then
        assertTrue(result.contains("retryLimit=5"));
        assertTrue(result.contains("PT3S"));
        assertTrue(result.contains("parallel=true"));
        assertTrue(result.contains("recoverOnFailure=true"));
        assertTrue(result.contains("PT1M"));
        assertTrue(result.contains("jitter=true"));
        assertTrue(result.contains("backpressureBufferCapacity=2048"));
        assertTrue(result.contains("backpressureStrategy=DROP"));
    }

    @Test
    void testFluentApiChaining() {
        // Given
        StepConfig config = new StepConfig();

        // When - chain multiple setters
        StepConfig result = config.retryLimit(10)
                .retryWait(Duration.ofMillis(500))
                .parallel(true)
                .recoverOnFailure(true)
                .maxBackoff(Duration.ofMinutes(5))
                .jitter(true)
                .backpressureBufferCapacity(4096)
                .backpressureStrategy("DROP");

        // Then
        assertSame(config, result);
        assertEquals(10, config.retryLimit());
        assertEquals(Duration.ofMillis(500), config.retryWait());
        assertTrue(config.parallel());
        assertTrue(config.recoverOnFailure());
        assertEquals(Duration.ofMinutes(5), config.maxBackoff());
        assertTrue(config.jitter());
        assertEquals(4096, config.backpressureBufferCapacity());
        assertEquals("DROP", config.backpressureStrategy());
    }

    @Test
    void testConfigFromPipelineStepConfigWithMinimalValues() {
        // Given
        PipelineStepConfig.StepConfig mockStepConfig = new PipelineStepConfig.StepConfig() {
            @Override
            public Integer retryLimit() {
                return 0;
            }

            @Override
            public Long retryWaitMs() {
                return 1L;
            }

            @Override
            public Boolean parallel() {
                return false;
            }

            @Override
            public Boolean recoverOnFailure() {
                return false;
            }

            @Override
            public Long maxBackoff() {
                return 1L;
            }

            @Override
            public Boolean jitter() {
                return false;
            }

            @Override
            public Integer backpressureBufferCapacity() {
                return 1;
            }

            @Override
            public String backpressureStrategy() {
                return "BUFFER";
            }

            @Override
            public Integer order() {
                return 1;
            }
        };

        // When
        StepConfig config = new StepConfig(mockStepConfig);

        // Then
        assertEquals(0, config.retryLimit());
        assertEquals(Duration.ofMillis(1), config.retryWait());
        assertFalse(config.parallel());
        assertFalse(config.recoverOnFailure());
        assertEquals(Duration.ofMillis(1), config.maxBackoff());
        assertFalse(config.jitter());
        assertEquals(1, config.backpressureBufferCapacity());
        assertEquals("BUFFER", config.backpressureStrategy());
    }

    @Test
    void testConfigFromPipelineStepConfigWithMaximalValues() {
        // Given
        PipelineStepConfig.StepConfig mockStepConfig = new PipelineStepConfig.StepConfig() {
            @Override
            public Integer retryLimit() {
                return Integer.MAX_VALUE;
            }

            @Override
            public Long retryWaitMs() {
                return Long.MAX_VALUE;
            }

            @Override
            public Boolean parallel() {
                return true;
            }

            @Override
            public Boolean recoverOnFailure() {
                return true;
            }

            @Override
            public Long maxBackoff() {
                return Long.MAX_VALUE;
            }

            @Override
            public Boolean jitter() {
                return true;
            }

            @Override
            public Integer backpressureBufferCapacity() {
                return Integer.MAX_VALUE;
            }

            @Override
            public String backpressureStrategy() {
                return "DROP";
            }

            @Override
            public Integer order() {
                return Integer.MAX_VALUE;
            }
        };

        // When
        StepConfig config = new StepConfig(mockStepConfig);

        // Then
        assertEquals(Integer.MAX_VALUE, config.retryLimit());
        assertEquals(Duration.ofMillis(Long.MAX_VALUE), config.retryWait());
        assertTrue(config.parallel());
        assertTrue(config.recoverOnFailure());
        assertEquals(Duration.ofMillis(Long.MAX_VALUE), config.maxBackoff());
        assertTrue(config.jitter());
        assertEquals(Integer.MAX_VALUE, config.backpressureBufferCapacity());
        assertEquals("DROP", config.backpressureStrategy());
    }
}
