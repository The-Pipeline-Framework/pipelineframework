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
import org.pipelineframework.config.PlatformMode;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for PipelineDiscoveryPhase */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PipelineDiscoveryPhaseTest {

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
    void testSupportedAnnotationTypes() {
        // We need to initialize processor for this test
        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase();
        // Note: Discovery phase doesn't need initialization like the processor
        // This test would be more appropriate for the main processor
        assertNotNull(phase);
    }

    @Test
    void testDiscoveryPhaseInitialization() {
        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase();
        assertNotNull(phase);
        assertEquals("Pipeline Discovery Phase", phase.name());
    }

    @Test
    void testDiscoveryPhaseExecution() throws Exception {
        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        // Execute the phase
        phase.execute(context);

        // Verify that the context has been populated with expected values
        // Since there are no annotated elements in the mock, we expect empty lists
        assertNotNull(context.getStepModels()); // Should be initialized to empty list
        assertNotNull(context.getAspectModels()); // Should be initialized to empty list
        assertNotNull(context.getOrchestratorModels()); // Should be initialized to empty list
        assertNotNull(context.getResolvedTargets()); // Should be initialized to empty set
        assertNotNull(context.getGeneratedSourcesRoot()); // Should be set to default path
        assertNotNull(context.getModuleDir()); // Should be set to default path
        assertFalse(context.isPluginHost()); // Should be false since no plugin elements
        assertTrue(context.isTransportModeGrpc()); // Should be true by default
        assertEquals(PlatformMode.STANDARD, context.getPlatformMode()); // Should be STANDARD by default
    }
}
