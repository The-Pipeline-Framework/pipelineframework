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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.processor.PipelineCompilationContext;

import static org.junit.jupiter.api.Assertions.*;
import static javax.tools.Diagnostic.Kind.NOTE;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for ModelExtractionPhase */
@ExtendWith(MockitoExtension.class)
class ModelExtractionPhaseTest {

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private RoundEnvironment roundEnv;

    @Mock
    private Messager messager;

    @BeforeEach
    void setUp() {
        lenient().when(processingEnv.getMessager()).thenReturn(messager);
        lenient().when(processingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        lenient().when(processingEnv.getFiler()).thenReturn(mock(javax.annotation.processing.Filer.class));
        lenient().when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);
        lenient().when(roundEnv.getElementsAnnotatedWith(PipelineStep.class)).thenReturn(Set.of());
    }

    @Test
    void testModelExtractionPhaseInitialization() {
        ModelExtractionPhase phase = new ModelExtractionPhase();
        assertNotNull(phase);
        assertEquals("Model Extraction Phase", phase.name());
    }

    @Test
    void testConstructorInjectionRejectsNullRoleEnricher() {
        assertThrows(NullPointerException.class, () -> new ModelExtractionPhase(null));
    }

    @Test
    void testExecution_noAnnotatedElements_emptyModels() throws Exception {
        ModelExtractionPhase phase = new ModelExtractionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        phase.execute(context);

        assertNotNull(context.getStepModels());
        assertTrue(context.getStepModels().isEmpty());
    }

    @Test
    void testExecution_noTemplateConfig_noAnnotationModels() throws Exception {
        ModelExtractionPhase phase = new ModelExtractionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setPipelineTemplateConfig(null);

        phase.execute(context);

        assertTrue(context.getStepModels().isEmpty());
    }

    @Test
    void testExecution_emitsNoteWhenFallingBackToLegacyExtraction() throws Exception {
        ModelExtractionPhase phase = new ModelExtractionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setStepDefinitions(List.of());

        phase.execute(context);

        verify(messager).printMessage(
            NOTE,
            ModelExtractionPhase.NO_YAML_DEFINITIONS_MESSAGE);
    }

    @Test
    void testExecute_withTemplateModels_doesNotGenerateWithoutYamlStepDefinitions() throws Exception {
        ModelExtractionPhase phase = new ModelExtractionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        // Create a real PipelineTemplateStep record instance
        var templateStep = new org.pipelineframework.config.template.PipelineTemplateStep(
            "TestStep",
            "ONE_TO_ONE",  // Valid cardinality string, not a StreamingShape value
            "InputType",
            java.util.List.of(),
            "OutputType",
            java.util.List.of()
        );

        // Create a real PipelineTemplateConfig record instance
        var templateConfig = new org.pipelineframework.config.template.PipelineTemplateConfig(
            "testApp",
            "com.example",
            "GRPC",
            java.util.List.of(templateStep),
            java.util.Map.of()
        );

        context.setPipelineTemplateConfig(templateConfig);
        context.setPluginHost(true);  // Need to be a plugin host or have orchestrator to process templates
        context.setTransportMode(org.pipelineframework.processor.ir.TransportMode.LOCAL);  // Make plugins colocated to avoid needing plugin aspects

        phase.execute(context);

        // Template-only synthesis is disabled in YAML-driven mode.
        assertNotNull(context.getStepModels());
        assertTrue(context.getStepModels().isEmpty(), "Expected no generated step models without YAML step definitions");
    }
}
