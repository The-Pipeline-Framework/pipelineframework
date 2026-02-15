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

package org.pipelineframework.processor.phase;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineStepModel;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for TemplateModelBuilder */
class TemplateModelBuilderTest {

    private final TemplateModelBuilder builder = new TemplateModelBuilder();

    @Test
    void buildModels_nullConfig_empty() {
        assertTrue(builder.buildModels(null).isEmpty());
    }

    @Test
    void buildModels_nullSteps_empty() {
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", null, null, null);
        assertTrue(builder.buildModels(config).isEmpty());
    }

    @Test
    void buildModels_emptySteps_empty() {
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", null, List.of(), null);
        assertTrue(builder.buildModels(config).isEmpty());
    }

    @Test
    void buildModels_blankBasePackage_empty() {
        PipelineTemplateStep step = step("Process Data", "Input", "Output");
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "", null, List.of(step), null);
        assertTrue(builder.buildModels(config).isEmpty());
    }

    @Test
    void buildModels_validStep_buildsModel() {
        PipelineTemplateStep step = step("Process Data", "InputDto", "OutputDto");
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", null, List.of(step), null);

        List<PipelineStepModel> models = builder.buildModels(config);

        assertEquals(1, models.size());
        PipelineStepModel model = models.getFirst();
        assertEquals("ProcessDataService", model.serviceName());
        assertEquals("com.example.process_data.service", model.servicePackage());
        assertEquals(DeploymentRole.PIPELINE_SERVER, model.deploymentRole());
    }

    @Test
    void buildModels_nullStep_skipped() {
        List<PipelineTemplateStep> steps = new java.util.ArrayList<>();
        steps.add(null);
        steps.add(step("Process Item", "In", "Out"));
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", null, steps, null);

        List<PipelineStepModel> models = builder.buildModels(config);
        assertEquals(1, models.size());
    }

    @Test
    void buildModels_multipleSteps() {
        PipelineTemplateStep step1 = step("Process First", "A", "B");
        PipelineTemplateStep step2 = step("Process Second", "B", "C");
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", null, List.of(step1, step2), null);

        List<PipelineStepModel> models = builder.buildModels(config);
        assertEquals(2, models.size());
    }

    // --- Package segment derivation (tested indirectly through buildModels) ---

    @Test
    void buildModels_specialCharsInName_producesValidPackage() {
        PipelineTemplateStep step = step("Process--Data!!", "In", "Out");
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", null, List.of(step), null);

        List<PipelineStepModel> models = builder.buildModels(config);
        assertEquals(1, models.size());
        String pkg = models.getFirst().servicePackage();
        // Package segment should not start/end with underscore
        String segment = pkg.replace("com.example.", "").replace(".service", "");
        assertFalse(segment.startsWith("_"));
        assertFalse(segment.endsWith("_"));
        assertEquals("process_data", segment);
    }

    /**
     * Creates a test PipelineTemplateStep with the given name, input type, and output type.
     * Other fields (cardinality, inputFields, outputFields) are left null.
     */
    private static PipelineTemplateStep step(String name, String inputType, String outputType) {
        return new PipelineTemplateStep(name, null, inputType, null, outputType, null);
    }
}
