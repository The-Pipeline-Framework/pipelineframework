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
import java.util.Map;
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
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
    void testBindingConstructionPhaseExecution_noModels() throws Exception {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        assertDoesNotThrow(() -> phase.execute(context));
        assertNotNull(context.getRendererBindings());
        assertTrue(context.getRendererBindings().isEmpty());
    }

    @Test
    void testBindingConstructionPhaseExecution_stepWithRestTargets() throws Exception {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        PipelineStepModel modelWithTargets = TestModelFactory.createTestModelWithTargets("TestService", Set.of(GenerationTarget.REST_RESOURCE));
        context.setStepModels(List.of(modelWithTargets));

        phase.execute(context);

        Map<String, Object> bindings = context.getRendererBindings();
        assertTrue(bindings.containsKey("TestService_rest"));
        assertFalse(bindings.containsKey("TestService_grpc"));
    }

    @Test
    void testBindingConstructionPhaseExecution_stepWithLocalTarget() throws Exception {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        PipelineStepModel modelWithTargets = TestModelFactory.createTestModelWithTargets("TestService", Set.of(GenerationTarget.LOCAL_CLIENT_STEP));
        context.setStepModels(List.of(modelWithTargets));

        phase.execute(context);

        Map<String, Object> bindings = context.getRendererBindings();
        assertTrue(bindings.containsKey("TestService_local"));
    }

    @Test
    void testConstructorInjection() {
        GrpcRequirementEvaluator evaluator = new GrpcRequirementEvaluator();
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase(evaluator);
        assertNotNull(phase);
    }

    @Test
    void testConstructorInjection_rejectsNull() {
        assertThrows(NullPointerException.class,
            () -> new PipelineBindingConstructionPhase(null));
        assertThrows(NullPointerException.class,
            () -> new PipelineBindingConstructionPhase(new GrpcRequirementEvaluator(), null));
    }

    @Test
    void delegatedClientStepDoesNotRequireGrpcDescriptorBindings() throws Exception {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        PipelineStepModel delegatedModel = TestModelFactory
            .createTestModelWithTargets("DelegatedService", Set.of(GenerationTarget.CLIENT_STEP))
            .toBuilder()
            .delegateService(ClassName.get("com.example.lib", "EmbeddingService"))
            .build();
        context.setStepModels(List.of(delegatedModel));

        assertDoesNotThrow(() -> phase.execute(context));
        Map<String, Object> bindings = context.getRendererBindings();
        assertTrue(bindings.containsKey("DelegatedService_external_adapter"));
        assertFalse(bindings.containsKey("DelegatedService_grpc"));
    }

    @Test
    void delegatedLocalClientStepBuildsLocalAndExternalAdapterBindings() throws Exception {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        PipelineStepModel delegatedModel = TestModelFactory
            .createTestModelWithTargets("DelegatedLocalService", Set.of(GenerationTarget.LOCAL_CLIENT_STEP))
            .toBuilder()
            .delegateService(ClassName.get("com.example.lib", "EmbeddingService"))
            .build();
        context.setStepModels(List.of(delegatedModel));

        assertDoesNotThrow(() -> phase.execute(context));
        Map<String, Object> bindings = context.getRendererBindings();
        assertTrue(bindings.containsKey("DelegatedLocalService_external_adapter"));
        assertTrue(bindings.containsKey("DelegatedLocalService_local"));
        assertFalse(bindings.containsKey("DelegatedLocalService_grpc"));
    }

    @Test
    void delegatedStepWithServerTargetsEmitsWarning() throws Exception {
        PipelineBindingConstructionPhase phase = new PipelineBindingConstructionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        PipelineStepModel delegatedModel = TestModelFactory
            .createTestModelWithTargets("DelegatedServerTargetService", Set.of(
                GenerationTarget.GRPC_SERVICE,
                GenerationTarget.REST_RESOURCE,
                GenerationTarget.LOCAL_CLIENT_STEP))
            .toBuilder()
            .delegateService(ClassName.get("com.example.lib", "EmbeddingService"))
            .build();
        context.setStepModels(List.of(delegatedModel));

        phase.execute(context);

        verify(messager).printMessage(
            eq(javax.tools.Diagnostic.Kind.WARNING),
            contains("Delegated step 'DelegatedServerTargetService' ignores server targets"));
    }
}
