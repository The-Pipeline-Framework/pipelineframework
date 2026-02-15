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
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for TemplateExpansionOrchestrator */
@ExtendWith(MockitoExtension.class)
class TemplateExpansionOrchestratorTest {

    private final TemplateExpansionOrchestrator orchestrator = new TemplateExpansionOrchestrator();

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private RoundEnvironment roundEnv;

    @Test
    void expandTemplateModels_emptyBaseModels_empty() {
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        assertTrue(orchestrator.expandTemplateModels(ctx, List.of()).isEmpty());
    }

    @Test
    void expandTemplateModels_noOrchestratorNotPluginHost_empty() {
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setPluginHost(false);

        List<PipelineStepModel> baseModels = List.of(createTestModel("TestService"));

        assertTrue(orchestrator.expandTemplateModels(ctx, baseModels).isEmpty());
    }

    private PipelineStepModel createTestModel(String name) {
        return new PipelineStepModel(
            name, name, "com.example.service", ClassName.get("com.example.service", name),
            new TypeMapping(null, null, false), new TypeMapping(null, null, false),
            StreamingShape.UNARY_UNARY, Set.of(), ExecutionMode.DEFAULT,
            DeploymentRole.PIPELINE_SERVER, false, null);
    }
}
