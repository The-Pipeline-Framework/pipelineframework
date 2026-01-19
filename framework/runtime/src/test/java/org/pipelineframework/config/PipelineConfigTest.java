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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PipelineConfigTest {

    @Test
    void testPipelineConfigDefaults() {
        // Given
        PipelineConfig pipelineConfig = new PipelineConfig();

        // When
        StepConfig defaults = pipelineConfig.defaults();

        // Then
        assertNotNull(defaults);
        assertEquals(3, defaults.retryLimit());
    }

    @Test
    void testPipelineConfigProfileManagement() {
        // Given
        PipelineConfig pipelineConfig = new PipelineConfig();

        // When
        pipelineConfig.profile("test", new StepConfig().retryLimit(5));
        pipelineConfig.activate("test");

        StepConfig activeConfig = pipelineConfig.defaults();

        // Then
        assertEquals("test", pipelineConfig.activeProfile());
        assertEquals(5, activeConfig.retryLimit());
    }

    @Test
    void testNewStepConfigInheritsDefaults() {
        // Given
        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.defaults().retryLimit(7);

        // When
        StepConfig stepConfig = pipelineConfig.newStepConfig();

        // Then
        assertEquals(7, stepConfig.retryLimit());
    }
}
