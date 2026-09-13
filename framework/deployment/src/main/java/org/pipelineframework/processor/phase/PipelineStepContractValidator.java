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
import java.util.Objects;
import java.util.Optional;

import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Validates Java-domain compatibility after service and operator contracts have been extracted.
 */
final class PipelineStepContractValidator {

    void validate(PipelineCompilationContext ctx, List<PipelineStepModel> models) {
        validateCallbackInvocationShape(ctx, models);
        if (ctx == null
            || !(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config)
            || config.inputContract() == null
            || models == null
            || models.size() < 2
            || (!config.steps().isEmpty() && models.size() != config.steps().size())
            || (ctx.getBranchingPlan() != null && ctx.getBranchingPlan().branchAware())
            || ctx.getProcessingEnv() == null) {
            return;
        }
        for (int index = 1; index < models.size(); index++) {
            PipelineStepModel previous = models.get(index - 1);
            PipelineStepModel current = models.get(index);
            Optional<TypeName> previousOutput = domainOutput(previous);
            Optional<TypeName> currentInput = domainInput(current);
            if (previousOutput.isEmpty() || currentInput.isEmpty()
                || Objects.equals(previousOutput.get(), currentInput.get())) {
                continue;
            }
            ctx.getCompilerDiagnostics().error(
                "Step '" + stepName(config, index, current) + "' resolves Java input '" + currentInput.get()
                    + "', but previous step '" + stepName(config, index - 1, previous)
                    + "' resolves Java output '"
                    + previousOutput.get() + "'.");
        }
    }

    private void validateCallbackInvocationShape(PipelineCompilationContext ctx, List<PipelineStepModel> models) {
        if (ctx == null || models == null || ctx.getProcessingEnv() == null) {
            return;
        }
        if (ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config
            && !config.steps().isEmpty() && models.size() != config.steps().size()) {
            return; // Cross-module subsets cannot establish invocation occurrence shape.
        }
        if (ctx.getBranchingPlan() != null && ctx.getBranchingPlan().branchAware()) {
            validateBranchCallbackInvocationShape(ctx, models);
            return;
        }
        boolean streaming = false;
        for (PipelineStepModel model : models) {
            if (model == null) {
                continue;
            }
            if (streaming && model.deferredCompletionSelection()
                .filter(completion -> completion.callback().isPresent()).isPresent()) {
                ctx.getCompilerDiagnostics().error(
                    "Command callback step '" + model.serviceName()
                        + "' cannot consume an upstream stream; aggregate into one canonical value before dispatch.");
            }
            if (model.streamingShape() == org.pipelineframework.processor.ir.StreamingShape.UNARY_STREAMING
                || model.streamingShape() == org.pipelineframework.processor.ir.StreamingShape.STREAMING_STREAMING) {
                streaming = true;
            } else if (model.streamingShape() == org.pipelineframework.processor.ir.StreamingShape.STREAMING_UNARY) {
                streaming = false;
            }
        }
    }

    private void validateBranchCallbackInvocationShape(PipelineCompilationContext ctx, List<PipelineStepModel> models) {
        if (models.size() != ctx.getBranchingPlan().steps().size()) {
            return;
        }
        java.util.Map<String, Boolean> streamingContracts = new java.util.HashMap<>();
        for (var step : ctx.getBranchingPlan().steps()) {
            var model = models.get(step.index());
            boolean streamingInput = step.acceptedContractTypes().stream()
                .anyMatch(contract -> streamingContracts.getOrDefault(contract, false));
            if (streamingInput && model.deferredCompletionSelection()
                .filter(completion -> completion.callback().isPresent()).isPresent()) {
                ctx.getCompilerDiagnostics().error(
                    "Command callback step '" + model.serviceName()
                        + "' cannot consume an upstream stream; aggregate into one canonical value before dispatch.");
            }
            boolean streamingOutput = switch (model.streamingShape()) {
                case UNARY_STREAMING, STREAMING_STREAMING -> true;
                case STREAMING_UNARY -> false;
                case UNARY_UNARY -> streamingInput;
            };
            step.acceptedContractTypes().forEach(streamingContracts::remove);
            step.producedLeafContractTypes().forEach(contract ->
                streamingContracts.merge(contract, streamingOutput, (left, right) -> left || right));
        }
    }

    private Optional<TypeName> domainInput(PipelineStepModel model) {
        return model == null || model.inputMapping() == null
            ? Optional.empty()
            : Optional.ofNullable(model.inputMapping().domainType());
    }

    private Optional<TypeName> domainOutput(PipelineStepModel model) {
        return model == null || model.outputMapping() == null
            ? Optional.empty()
            : Optional.ofNullable(model.pipelineOutputType());
    }

    private String stepName(PipelineTemplateConfig config, int index, PipelineStepModel fallback) {
        if (index >= 0 && index < config.steps().size() && config.steps().get(index) != null) {
            return config.steps().get(index).name();
        }
        return fallback.serviceName();
    }
}
