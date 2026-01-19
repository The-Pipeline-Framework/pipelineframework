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

import java.util.Map;
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for PipelineStepConfig configuration mapping interface. Tests the
 * configuration loading from application.properties and default values.
 */
@QuarkusTest
class PipelineStepConfigTest {

    @Inject
    PipelineStepConfig pipelineStepConfig;

    @Test
    void testDefaultsAreAccessible() {
        // When
        PipelineStepConfig.StepConfig defaults = pipelineStepConfig.defaults();

        // Then
        assertNotNull(defaults, "Defaults should not be null");
    }

    @Test
    void testDefaultsHaveCorrectDefaultValues() {
        // Given
        PipelineStepConfig.StepConfig defaults = pipelineStepConfig.defaults();

        // Then - verify all default values from @WithDefault annotations
        assertEquals(3, defaults.retryLimit(), "Default retryLimit should be 3");
        assertEquals(2000L, defaults.retryWaitMs(), "Default retryWaitMs should be 2000");
        assertFalse(defaults.recoverOnFailure(), "Default recoverOnFailure should be false");
        assertEquals(30000L, defaults.maxBackoff(), "Default maxBackoff should be 30000");
        assertFalse(defaults.jitter(), "Default jitter should be false");
        assertEquals(
                1024,
                defaults.backpressureBufferCapacity(),
                "Default backpressureBufferCapacity should be 1024");
        assertEquals(
                "BUFFER",
                defaults.backpressureStrategy(),
                "Default backpressureStrategy should be BUFFER");
    }

    @Test
    void testStepMapIsAccessible() {
        // When
        Map<String, PipelineStepConfig.StepConfig> stepMap = pipelineStepConfig.step();

        // Then
        assertNotNull(stepMap, "Step map should not be null");
    }

    @Test
    void testStepMapIsEmptyByDefault() {
        // When
        Map<String, PipelineStepConfig.StepConfig> stepMap = pipelineStepConfig.step();

        // Then - without specific configuration, map should be empty
        // Note: This test may fail if any pipeline.step.* entries are added to the
        // application.properties, but that's expected behavior for test isolation
        assertTrue(
                stepMap.isEmpty(), "Step map should be empty without specific step configuration");
    }

    /** Test profile for custom property values */
    public static class CustomDefaultsProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "pipeline.defaults.retry-limit", "10",
                    "pipeline.defaults.retry-wait-ms", "5000",
                    "pipeline.defaults.recover-on-failure", "true",
                    "pipeline.defaults.max-backoff", "60000",
                    "pipeline.defaults.jitter", "true",
                    "pipeline.defaults.backpressure-buffer-capacity", "2048",
                    "pipeline.defaults.backpressure-strategy", "DROP");
        }
    }

    @QuarkusTest
    @TestProfile(CustomDefaultsProfile.class)
    static class WithCustomDefaultsTest {

        @Inject
        PipelineStepConfig pipelineStepConfig;

        @Test
        void testCustomDefaultValues() {
            // Given
            PipelineStepConfig.StepConfig defaults = pipelineStepConfig.defaults();

            // Then - verify custom values are loaded
            assertEquals(10, defaults.retryLimit());
            assertEquals(5000L, defaults.retryWaitMs());
            assertTrue(defaults.recoverOnFailure());
            assertEquals(60000L, defaults.maxBackoff());
            assertTrue(defaults.jitter());
            assertEquals(2048, defaults.backpressureBufferCapacity());
            assertEquals("DROP", defaults.backpressureStrategy());
        }
    }

    /** Test profile for per-step configuration */
    public static class PerStepConfigProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "pipeline.step.\"com.example.MyStep\".retry-limit", "7",
                    "pipeline.step.\"com.example.AnotherStep\".retry-limit", "15");
        }
    }

    @QuarkusTest
    @TestProfile(PerStepConfigProfile.class)
    static class WithPerStepConfigTest {

        @Inject
        PipelineStepConfig pipelineStepConfig;

        @Test
        void testPerStepConfigurationIsLoaded() {
            // Given
            Map<String, PipelineStepConfig.StepConfig> stepMap = pipelineStepConfig.step();

            // Then - verify step-specific configuration
            assertNotNull(stepMap);
            assertTrue(
                    stepMap.containsKey("com.example.MyStep"),
                    "Should contain MyStep configuration");
            assertTrue(
                    stepMap.containsKey("com.example.AnotherStep"),
                    "Should contain AnotherStep configuration");

            PipelineStepConfig.StepConfig myStepConfig = stepMap.get("com.example.MyStep");
            assertEquals(7, myStepConfig.retryLimit());

            PipelineStepConfig.StepConfig anotherStepConfig = stepMap.get("com.example.AnotherStep");
            assertEquals(15, anotherStepConfig.retryLimit());
        }

        @Test
        void testPerStepConfigInheritsDefaultsForUnsetProperties() {
            // Given
            Map<String, PipelineStepConfig.StepConfig> stepMap = pipelineStepConfig.step();
            PipelineStepConfig.StepConfig myStepConfig = stepMap.get("com.example.MyStep");
            PipelineStepConfig.StepConfig defaults = pipelineStepConfig.defaults();

            // Then - unset properties should use interface defaults
            assertEquals(
                    defaults.retryWaitMs(),
                    myStepConfig.retryWaitMs(),
                    "Should use default retryWaitMs");
            assertEquals(
                    defaults.maxBackoff(),
                    myStepConfig.maxBackoff(),
                    "Should use default maxBackoff");
            assertEquals(defaults.jitter(), myStepConfig.jitter(), "Should use default jitter");
            assertEquals(
                    defaults.backpressureBufferCapacity(),
                    myStepConfig.backpressureBufferCapacity(),
                    "Should use default backpressureBufferCapacity");
            assertEquals(
                    defaults.backpressureStrategy(),
                    myStepConfig.backpressureStrategy(),
                    "Should use default backpressureStrategy");
        }
    }
}
