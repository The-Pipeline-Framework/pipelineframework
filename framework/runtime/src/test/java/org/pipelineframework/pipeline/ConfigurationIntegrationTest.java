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

package org.pipelineframework.pipeline;

import java.time.Duration;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.blocking.StepOneToOneBlocking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConfigurationIntegrationTest {

    static class ConfigTestStepBlocking extends ConfigurableStep
            implements StepOneToOneBlocking<String, String> {
        @Override
        public Uni<String> apply(String input) {
            // This is a blocking operation that simulates processing
            try {
                Thread.sleep(10); // Simulate some work
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Uni.createFrom().item("ConfigTest: " + input);
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            super.initialiseWithConfig(config);
        }

        public Uni<String> applyOneToOne(String input) {
            return apply(input);
        }
    }

    @Test
    void testPipelineConfigDefaults() {
        try (PipelineRunner runner = new PipelineRunner()) {
            PipelineConfig pipelineConfig = new PipelineConfig();

            // Check default values
            StepConfig defaults = pipelineConfig.defaults();
            assertEquals(3, defaults.retryLimit());
            assertEquals(Duration.ofMillis(2000), defaults.retryWait());

            assertFalse(defaults.recoverOnFailure());
            assertEquals(Duration.ofSeconds(30), defaults.maxBackoff());
            assertFalse(defaults.jitter());
        }
    }

    @Test
    void testPipelineConfigProfileManagement() {
        try (PipelineRunner runner = new PipelineRunner()) {
            PipelineConfig pipelineConfig = new PipelineConfig();

            // Set up profiles
            pipelineConfig.profile("test", new StepConfig().retryLimit(5));
            pipelineConfig.activate("test");

            // Verify active profile
            assertEquals("test", pipelineConfig.activeProfile());

            StepConfig activeConfig = pipelineConfig.defaults();
            assertEquals(5, activeConfig.retryLimit());
        }
    }

    @Test
    void testNewStepConfigInheritsDefaults() {
        try (PipelineRunner runner = new PipelineRunner()) {
            PipelineConfig pipelineConfig = new PipelineConfig();
            pipelineConfig.defaults().retryLimit(8);

            StepConfig stepConfig = pipelineConfig.newStepConfig();

            assertEquals(8, stepConfig.retryLimit());
        }
    }
}
