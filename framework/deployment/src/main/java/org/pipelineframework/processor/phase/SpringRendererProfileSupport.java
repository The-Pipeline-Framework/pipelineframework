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
import org.pipelineframework.processor.ir.PipelineStepModel;
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
        if (!ctx.isTransportModeLocal()) {
            errors.add("Spring renderer profile currently supports only pipeline.transport=LOCAL.");
        }
        if (ctx.isPlatformModeFunction()) {
            errors.add("Spring renderer profile currently supports only pipeline.platform=COMPUTE.");
        }
        if (ctx.isOrchestratorGenerated()) {
            errors.add("Spring renderer profile does not yet support generated orchestrator artifacts.");
        }

        for (PipelineStepModel model : ctx.getStepModels()) {
            validateModel(model, errors);
        }

        if (!errors.isEmpty()) {
            String message = errors.stream().distinct().collect(Collectors.joining(" "));
            if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(Diagnostic.Kind.ERROR, message);
            }
            throw new IllegalStateException(message);
        }
    }

    private static void validateModel(PipelineStepModel model, List<String> errors) {
        Set<GenerationTarget> supportedTargets = Set.of(GenerationTarget.LOCAL_CLIENT_STEP);
        if (!supportedTargets.containsAll(model.enabledTargets())) {
            errors.add("Spring renderer profile currently supports only LOCAL_CLIENT_STEP generation; step '"
                + model.serviceName() + "' resolved targets " + model.enabledTargets() + ".");
        }
        if (model.streamingShape() != StreamingShape.UNARY_UNARY) {
            errors.add("Spring renderer profile currently supports only unary-unary steps; step '"
                + model.serviceName() + "' has shape " + model.streamingShape() + ".");
        }
        if (model.serviceApiKind() != ServiceApiKind.REACTIVE) {
            errors.add("Spring renderer profile currently supports only reactive-authored services; step '"
                + model.serviceName() + "' has API kind " + model.serviceApiKind() + ".");
        }
        if (model.sideEffect()) {
            errors.add("Spring renderer profile does not yet support side-effect steps; step '"
                + model.serviceName() + "'.");
        }
        if (model.delegateService() != null || model.remoteExecution() != null) {
            errors.add("Spring renderer profile currently supports only internal local steps; step '"
                + model.serviceName() + "'.");
        }
    }
}
