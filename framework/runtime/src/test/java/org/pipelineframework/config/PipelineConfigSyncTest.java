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

import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.ConfigFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class PipelineConfigSyncTest {

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    ConfigFactory configFactory;

    @Test
    void testConfigFactoryUsesPipelineConfigDefaults() throws IllegalAccessException {
        // This test verifies that ConfigFactory now properly uses PipelineConfig's defaults
        // which have been initialized with values from application.properties

        // Before our fix, ConfigFactory would get defaults directly from PipelineStepConfig
        // After our fix, ConfigFactory should get defaults from PipelineConfig.newStepConfig()
        // which contains the properly initialized values

        // Create a mock step class for testing
        Class<?> mockStepClass = String.class; // Using String as a placeholder

        // Get config from the factory when there's no specific config for the class
        org.pipelineframework.config.StepConfig factoryConfig = configFactory.buildConfig(mockStepClass, pipelineConfig);

        // Get a new config from pipelineConfig directly
        org.pipelineframework.config.StepConfig directConfig = pipelineConfig.newStepConfig();

        // With our fix, both configs should have the same values
        // as they both now come from the properly initialized PipelineConfig
        assertEquals(
                directConfig.retryLimit(),
                factoryConfig.retryLimit(),
                "ConfigFactory should return config with same retryLimit as PipelineConfig");
        assertEquals(
                directConfig.retryWait(),
                factoryConfig.retryWait(),
                "ConfigFactory should return config with same retryWait as PipelineConfig");
        assertEquals(
                directConfig.recoverOnFailure(),
                factoryConfig.recoverOnFailure(),
                "ConfigFactory should return config with same recoverOnFailure as PipelineConfig");
        assertEquals(
                directConfig.maxBackoff(),
                factoryConfig.maxBackoff(),
                "ConfigFactory should return config with same maxBackoff as PipelineConfig");
        assertEquals(
                directConfig.jitter(),
                factoryConfig.jitter(),
                "ConfigFactory should return config with same jitter as PipelineConfig");
        assertEquals(
                directConfig.backpressureBufferCapacity(),
                factoryConfig.backpressureBufferCapacity(),
                "ConfigFactory should return config with same backpressureBufferCapacity as PipelineConfig");
        assertEquals(
                directConfig.backpressureStrategy(),
                factoryConfig.backpressureStrategy(),
                "ConfigFactory should return config with same backpressureStrategy as PipelineConfig");
    }
}
