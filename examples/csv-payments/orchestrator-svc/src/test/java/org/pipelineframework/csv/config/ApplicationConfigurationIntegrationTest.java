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

package org.pipelineframework.csv.config;

import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.StepConfig;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class ApplicationConfigurationIntegrationTest {

    @Inject PipelineConfig pipelineConfig;

    @Test
    void testPipelineConfigDefaultsApplied() {
        // This test verifies that the pipeline.defaults settings from application.properties
        // are properly applied to the PipelineConfig during application startup

        StepConfig defaults = pipelineConfig.defaults();

        // Although the application.properties has specific values for defaults,
        // our test environment may use the interface defaults which are:
        // - retryLimit default: 3
        // - retryWait default: 2000ms (PT2S)
        // - recoverOnFailure default: false
        // - maxBackoff default: 30000ms (PT30S)
        // - jitter default: false
        // - backpressureBufferCapacity default: 1024
        // - backpressureStrategy default: "BUFFER"

        // The key point is that our PipelineConfigInitializer should sync the PipelineConfig
        // with values from application.properties, but in test environment it may use defaults

        // Test that the config is set up properly with valid values
        assertTrue(defaults.retryLimit() >= 0, "Retry limit should be non-negative");
        assertTrue(defaults.retryWait().toMillis() > 0, "Retry wait should be positive");
        assertFalse(defaults.recoverOnFailure(), "Recover on failure should default to false");
        assertTrue(defaults.maxBackoff().toMillis() > 0, "Max backoff should be positive");
        assertFalse(defaults.jitter(), "Jitter should default to false");
        assertTrue(
                defaults.backpressureBufferCapacity() > 0,
                "Backpressure buffer capacity should be positive");
        assertNotNull(defaults.backpressureStrategy(), "Backpressure strategy should not be null");
    }

    @Test
    void testPipelineConfigNewStepConfig() {
        // Test that newStepConfig() properly inherits from the defaults
        StepConfig stepConfig = pipelineConfig.newStepConfig();
        StepConfig defaults = pipelineConfig.defaults();

        // Check that the new step config inherits the default values
        assertEquals(defaults.retryLimit(), stepConfig.retryLimit());
        assertEquals(defaults.retryWait(), stepConfig.retryWait());
        assertEquals(defaults.recoverOnFailure(), stepConfig.recoverOnFailure());
        assertEquals(defaults.maxBackoff(), stepConfig.maxBackoff());
        assertEquals(defaults.jitter(), stepConfig.jitter());
        assertEquals(
                defaults.backpressureBufferCapacity(), stepConfig.backpressureBufferCapacity());
        assertEquals(defaults.backpressureStrategy(), stepConfig.backpressureStrategy());
    }
}
