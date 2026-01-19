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

package org.pipelineframework.pipeline.config;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;

import static org.junit.jupiter.api.Assertions.*;

class StepConfigTest {

    @Test
    void testDefaultValues() {
        // Given
        StepConfig config = new StepConfig();

        // Then
        assertEquals(3, config.retryLimit());
        assertEquals(Duration.ofMillis(2000), config.retryWait());
        assertFalse(config.recoverOnFailure());
        assertEquals(Duration.ofSeconds(30), config.maxBackoff());
        assertFalse(config.jitter());
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
}
