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
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/** Unit tests for GrpcRequirementEvaluator */
@ExtendWith(MockitoExtension.class)
class GrpcRequirementEvaluatorTest {

    private final GrpcRequirementEvaluator evaluator = new GrpcRequirementEvaluator();

    @Mock
    private Messager messager;

    @Test
    void needsGrpc_stepWithGrpcService_true() {
        PipelineStepModel model = createModel("Svc", Set.of(GenerationTarget.GRPC_SERVICE));
        assertTrue(evaluator.needsGrpcBindings(List.of(model), List.of(), null, messager));
    }

    @Test
    void needsGrpc_stepWithClientStep_true() {
        PipelineStepModel model = createModel("Svc", Set.of(GenerationTarget.CLIENT_STEP));
        assertTrue(evaluator.needsGrpcBindings(List.of(model), List.of(), null, messager));
    }

    @Test
    void needsGrpc_stepWithRestOnly_false() {
        PipelineStepModel model = createModel("Svc", Set.of(GenerationTarget.REST_RESOURCE));
        assertFalse(evaluator.needsGrpcBindings(List.of(model), List.of(), null, messager));
    }

    @Test
    void needsGrpc_noModels_false() {
        assertFalse(evaluator.needsGrpcBindings(List.of(), List.of(), null, messager));
    }

    @Test
    void needsGrpc_orchestratorWithGrpcTemplate_true() {
        PipelineOrchestratorModel orch = new PipelineOrchestratorModel("OrcSvc", "com.example", Set.of(), false);
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", "GRPC", List.of(), null);

        assertTrue(evaluator.needsGrpcBindings(List.of(), List.of(orch), config, messager));
    }

    @Test
    void needsGrpc_orchestratorWithRestTemplate_false() {
        PipelineOrchestratorModel orch = new PipelineOrchestratorModel("OrcSvc", "com.example", Set.of(), false);
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", "REST", List.of(), null);

        assertFalse(evaluator.needsGrpcBindings(List.of(), List.of(orch), config, messager));
    }

    @Test
    void needsGrpc_orchestratorWithBlankTransport_trueDefault() {
        PipelineOrchestratorModel orch = new PipelineOrchestratorModel("OrcSvc", "com.example", Set.of(), false);
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", "", List.of(), null);

        assertTrue(evaluator.needsGrpcBindings(List.of(), List.of(orch), config, messager));
    }

    @Test
    void needsGrpc_orchestratorWithUnknownTransport_warnsAndFalse() {
        PipelineOrchestratorModel orch = new PipelineOrchestratorModel("OrcSvc", "com.example", Set.of(), false);
        PipelineTemplateConfig config = new PipelineTemplateConfig("app", "com.example", "UNKNOWN", List.of(), null);

        assertFalse(evaluator.needsGrpcBindings(List.of(), List.of(orch), config, messager));
        verify(messager).printMessage(eq(Diagnostic.Kind.WARNING), contains("Unknown transport"));
    }

    @Test
    void needsGrpc_stepWithMixedTargets_true() {
        PipelineStepModel model = createModel("Svc", Set.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.REST_RESOURCE));
        assertTrue(evaluator.needsGrpcBindings(List.of(model), List.of(), null, messager));
    }

    @Test
    void needsGrpc_orchestratorNoConfig_false() {
        PipelineOrchestratorModel orch = new PipelineOrchestratorModel("OrcSvc", "com.example", Set.of(), false);
        assertFalse(evaluator.needsGrpcBindings(List.of(), List.of(orch), null, messager));
    }

    private PipelineStepModel createModel(String name, Set<GenerationTarget> targets) {
        return new PipelineStepModel(
            name, name, "com.example.service", ClassName.get("com.example.service", name),
            new TypeMapping(null, null, false), new TypeMapping(null, null, false),
            StreamingShape.UNARY_UNARY, targets, ExecutionMode.DEFAULT,
            DeploymentRole.PIPELINE_SERVER, false, null);
    }
}
