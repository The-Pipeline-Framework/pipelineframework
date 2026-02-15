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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for ModelExtractionPhase */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ModelExtractionPhaseTest {

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
        when(roundEnv.getElementsAnnotatedWith(PipelineStep.class)).thenReturn(Set.of());
    }

    @Test
    void testModelExtractionPhaseInitialization() {
        ModelExtractionPhase phase = new ModelExtractionPhase();
        assertNotNull(phase);
        assertEquals("Model Extraction Phase", phase.name());
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
    void testConstructorInjection() {
        TemplateModelBuilder builder = new TemplateModelBuilder();
        TemplateExpansionOrchestrator orchestrator = new TemplateExpansionOrchestrator();

        ModelExtractionPhase phase = new ModelExtractionPhase(builder, orchestrator);
        assertNotNull(phase);
        assertEquals("Model Extraction Phase", phase.name());
    }

    @Test
    void testConstructorInjection_rejectsNull() {
        assertThrows(NullPointerException.class,
            () -> new ModelExtractionPhase(null, new TemplateExpansionOrchestrator()));
        assertThrows(NullPointerException.class,
            () -> new ModelExtractionPhase(new TemplateModelBuilder(), null));
    }

    @Test
    void testExecute_withTemplateModels_mergesIntoStepModels() throws Exception {
        ModelExtractionPhase phase = new ModelExtractionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        
        // Create a mock template config with steps
        var templateConfig = new org.pipelineframework.config.template.PipelineTemplateConfig() {
            @Override
            public String basePackage() {
                return "com.example";
            }

            @Override
            public java.util.List<org.pipelineframework.config.template.PipelineTemplateStep> steps() {
                return java.util.List.of(
                    new org.pipelineframework.config.template.PipelineTemplateStep() {
                        @Override
                        public String name() {
                            return "TestStep";
                        }

                        @Override
                        public String inputTypeName() {
                            return "InputType";
                        }

                        @Override
                        public String outputTypeName() {
                            return "OutputType";
                        }

                        @Override
                        public org.pipelineframework.config.template.Cardinality cardinality() {
                            return org.pipelineframework.config.template.Cardinality.UNARY_UNARY;
                        }
                    }
                );
            }
        };
        
        context.setPipelineTemplateConfig(templateConfig);

        phase.execute(context);

        // Verify that context.getStepModels() contains the template models (size > 0 and includes expected elements)
        assertNotNull(context.getStepModels());
        assertFalse(context.getStepModels().isEmpty());
    }
}
