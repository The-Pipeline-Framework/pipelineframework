/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.tools.Diagnostic;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Validation and selection helpers for the narrow Spring renderer profile.
 */
final class SpringRendererProfileSupport {
    private static final String SPRING_PROFILE = "spring";

    private SpringRendererProfileSupport() {
    }

    static boolean isSpringProfile(PipelineCompilationContext ctx) {
        return ctx != null && SPRING_PROFILE.equalsIgnoreCase(ctx.getRendererProfile());
    }

    static void validateGenerationSupported(PipelineCompilationContext ctx) {
        if (!isSpringProfile(ctx)) {
            return;
        }

        List<String> errors = new ArrayList<>();
        PipelineTransport transportMode = ctx.getTransportMode();
        if (transportMode != PipelineTransport.LOCAL && transportMode != PipelineTransport.REST) {
            errors.add("Spring renderer profile currently supports only pipeline.transport=LOCAL or REST.");
        }
        if (ctx.isPlatformModeFunction()) {
            errors.add("Spring renderer profile currently supports only pipeline.platform=COMPUTE.");
        }
        if (ctx.isOrchestratorGenerated()) {
            errors.add("Spring renderer profile does not yet support generated orchestrator artifacts.");
        }

        for (PipelineStepModel model : ctx.getStepModels()) {
            validateModel(model, transportMode, errors);
        }

        if (!errors.isEmpty()) {
            String message = errors.stream().distinct().collect(Collectors.joining(" "));
            if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(Diagnostic.Kind.ERROR, message);
            }
            throw new IllegalStateException(message);
        }
    }

    private static void validateModel(PipelineStepModel model, PipelineTransport transportMode, List<String> errors) {
        Set<GenerationTarget> supportedTargets = transportMode == PipelineTransport.REST
            ? Set.of(GenerationTarget.LOCAL_CLIENT_STEP, GenerationTarget.REST_RESOURCE, GenerationTarget.REST_CLIENT_STEP)
            : Set.of(GenerationTarget.LOCAL_CLIENT_STEP);
        if (!supportedTargets.containsAll(model.enabledTargets())) {
            errors.add("Spring renderer profile currently supports only " + supportedTargets + " generation; step '"
                + model.serviceName() + "' resolved targets " + model.enabledTargets() + ".");
        }
        if (model.delegateService() != null && !model.enabledTargets().equals(Set.of(GenerationTarget.LOCAL_CLIENT_STEP))) {
            errors.add("Spring renderer profile supports delegated Spring beans only as unary local client steps; step '"
                + model.serviceName() + "' resolved targets " + model.enabledTargets() + ".");
        }
        if (model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)
            && model.deploymentRole() == DeploymentRole.PIPELINE_SERVER) {
            errors.add("Spring renderer profile supports REST client steps only for client-role boundaries; step '"
                + model.serviceName() + "' resolved role " + model.deploymentRole() + ".");
        }
        if (model.streamingShape() != StreamingShape.UNARY_UNARY) {
            errors.add("Spring renderer profile currently supports only unary-unary steps; step '"
                + model.serviceName() + "' has shape " + model.streamingShape() + ".");
        }
        if (model.serviceApiKind() != ServiceApiKind.REACTIVE
            && model.serviceApiKind() != ServiceApiKind.BLOCKING) {
            errors.add("Spring renderer profile currently supports only reactive or blocking unary services; step '"
                + model.serviceName() + "' has API kind " + model.serviceApiKind() + ".");
        }
        if (model.sideEffect()) {
            errors.add("Spring renderer profile does not yet support side-effect steps; step '"
                + model.serviceName() + "'.");
        }
        if (model.remoteExecution() != null) {
            errors.add("Spring renderer profile does not support remote execution; step '"
                + model.serviceName() + "'.");
        }
    }
}
