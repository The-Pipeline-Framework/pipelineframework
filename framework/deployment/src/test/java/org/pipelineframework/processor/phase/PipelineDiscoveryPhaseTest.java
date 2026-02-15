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
import org.pipelineframework.processor.PipelineCompilationContext;

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
    void testDiscoveryPhaseInitialization() {
        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase();
        assertNotNull(phase);
        assertEquals("Pipeline Discovery Phase", phase.name());
    }

    @Test
    void testDiscoveryPhaseExecution_defaultValues() throws Exception {
        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        phase.execute(context);

        assertNotNull(context.getGeneratedSourcesRoot());
        assertNotNull(context.getModuleDir());
        assertFalse(context.isPluginHost());
        assertTrue(context.isTransportModeGrpc());
        assertEquals(PlatformMode.COMPUTE, context.getPlatformMode());
        assertNotNull(context.getAspectModels());
        assertNotNull(context.getOrchestratorModels());
        assertTrue(context.getOrchestratorModels().isEmpty());
    }

    @Test
    void testDiscoveryPhaseExecution_nullRoundEnv() throws Exception {
        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, null);

        phase.execute(context);

        assertNotNull(context.getGeneratedSourcesRoot());
        assertFalse(context.isPluginHost());
        assertTrue(context.getOrchestratorModels().isEmpty());
    }

    @Test
    void testDiscoveryPhaseExecution_nullProcessingEnv() throws Exception {
        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);

        phase.execute(context);

        assertNotNull(context.getGeneratedSourcesRoot());
        assertNotNull(context.getModuleDir());
        assertFalse(context.isPluginHost());
    }

    @Test
    void testConstructorInjection() {
        DiscoveryPathResolver pathResolver = new DiscoveryPathResolver();
        DiscoveryConfigLoader configLoader = new DiscoveryConfigLoader();
        TransportPlatformResolver tpResolver = new TransportPlatformResolver();

        PipelineDiscoveryPhase phase = new PipelineDiscoveryPhase(pathResolver, configLoader, tpResolver);
        assertNotNull(phase);
        assertEquals("Pipeline Discovery Phase", phase.name());
    }

    @Test
    void testConstructorInjection_rejectsNull() {
        assertThrows(NullPointerException.class,
            () -> new PipelineDiscoveryPhase(null, new DiscoveryConfigLoader(), new TransportPlatformResolver()));
        assertThrows(NullPointerException.class,
            () -> new PipelineDiscoveryPhase(new DiscoveryPathResolver(), null, new TransportPlatformResolver()));
        assertThrows(NullPointerException.class,
            () -> new PipelineDiscoveryPhase(new DiscoveryPathResolver(), new DiscoveryConfigLoader(), null));
    }
}
