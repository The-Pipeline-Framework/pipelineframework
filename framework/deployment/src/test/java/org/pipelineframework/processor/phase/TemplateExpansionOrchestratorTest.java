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
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for TemplateExpansionOrchestrator */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TemplateExpansionOrchestratorTest {

    private final TemplateExpansionOrchestrator orchestrator = new TemplateExpansionOrchestrator();

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private RoundEnvironment roundEnv;

    @Mock
    private Messager messager;

    @BeforeEach
    void setUp() {
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(processingEnv.getFiler()).thenReturn(mock(javax.annotation.processing.Filer.class));
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);
    }

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
