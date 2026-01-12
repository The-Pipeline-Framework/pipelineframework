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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for PipelineBindingConstructionPhase */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PipelineBindingConstructionPhaseTest {

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
    void testBindingConstructionPhaseInitialization() {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        assertNotNull(phase);
        assertEquals("Pipeline Binding Construction Phase", phase.name());
    }

    @Test
    void testBindingConstructionPhaseExecution() throws Exception {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        org.pipelineframework.processor.PipelineCompilationContext context = 
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);
        
        assertDoesNotThrow(() -> phase.execute(context));
    }
}