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

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.ConfigFactory;

@QuarkusTest
class PipelineConfigWithApplicationPropertiesTest {

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    ConfigFactory configFactory;

    @Test
    void testPipelineConfigDefaultsFromApplicationProperties() {
        // The PipelineConfig should have been initialized with values from application.properties
        // through the PipelineConfigInitializer during application startup

        StepConfig defaults = pipelineConfig.defaults();

        // These values would come from application.properties if configured there
        // Using default values as defined in PipelineStepConfig interface:
        // retryLimit default: 3
        // retryWaitMs default: 2000ms (which becomes PT2S duration)
        // parallel default: false
        // recoverOnFailure default: false
        // maxBackoff default: 30000ms (which becomes PT30S duration)
        // jitter default: false
        // backpressureBufferCapacity default: 1024
        // backpressureStrategy default: "BUFFER"

        assertEquals(3, defaults.retryLimit(), "Default retryLimit should be 3");
        assertEquals(
                java.time.Duration.ofSeconds(2),
                defaults.retryWait(),
                "Default retryWait should be 2 seconds");
        assertFalse(defaults.parallel(), "Default parallel should be false");
        assertFalse(defaults.recoverOnFailure(), "Default recoverOnFailure should be false");
        assertEquals(
                java.time.Duration.ofSeconds(30),
                defaults.maxBackoff(),
                "Default maxBackoff should be 30 seconds");
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
    void testPipelineConfigNewStepConfigUsesDefaults() {
        // Test that newStepConfig() correctly creates a new StepConfig with the initialized
        // defaults
        StepConfig newConfig = pipelineConfig.newStepConfig();

        StepConfig defaults = pipelineConfig.defaults();

        assertEquals(
                defaults.retryLimit(),
                newConfig.retryLimit(),
                "newStepConfig should use same retryLimit as defaults");
        assertEquals(
                defaults.retryWait(),
                newConfig.retryWait(),
                "newStepConfig should use same retryWait as defaults");
        assertEquals(
                defaults.parallel(),
                newConfig.parallel(),
                "newStepConfig should use same parallel as defaults");
        assertEquals(
                defaults.recoverOnFailure(),
                newConfig.recoverOnFailure(),
                "newStepConfig should use same recoverOnFailure as defaults");
        assertEquals(
                defaults.maxBackoff(),
                newConfig.maxBackoff(),
                "newStepConfig should use same maxBackoff as defaults");
        assertEquals(
                defaults.jitter(),
                newConfig.jitter(),
                "newStepConfig should use same jitter as defaults");
        assertEquals(
                defaults.backpressureBufferCapacity(),
                newConfig.backpressureBufferCapacity(),
                "newStepConfig should use same backpressureBufferCapacity as defaults");
        assertEquals(
                defaults.backpressureStrategy(),
                newConfig.backpressureStrategy(),
                "newStepConfig should use same backpressureStrategy as defaults");
    }
}
