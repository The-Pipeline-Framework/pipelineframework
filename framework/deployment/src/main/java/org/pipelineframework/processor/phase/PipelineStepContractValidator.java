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
import javax.tools.Diagnostic;

import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Validates Java-domain compatibility after service and operator contracts have been extracted.
 */
final class PipelineStepContractValidator {

    void validate(PipelineCompilationContext ctx, List<PipelineStepModel> models) {
        if (ctx == null
            || !(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config)
            || config.inputContract() == null
            || models == null
            || models.size() < 2
            || ctx.getProcessingEnv() == null) {
            return;
        }
        for (int index = 1; index < models.size(); index++) {
            PipelineStepModel previous = models.get(index - 1);
            PipelineStepModel current = models.get(index);
            TypeName previousOutput = domainOutput(previous);
            TypeName currentInput = domainInput(current);
            if (previousOutput == null || currentInput == null || Objects.equals(previousOutput, currentInput)) {
                continue;
            }
            ctx.getProcessingEnv().getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Step '" + stepName(config, index, current) + "' resolves Java input '" + currentInput
                    + "', but previous step '" + stepName(config, index - 1, previous)
                    + "' resolves Java output '"
                    + previousOutput + "'.");
        }
    }

    private TypeName domainInput(PipelineStepModel model) {
        return model == null || model.inputMapping() == null ? null : model.inputMapping().domainType();
    }

    private TypeName domainOutput(PipelineStepModel model) {
        return model == null || model.outputMapping() == null ? null : model.outputMapping().domainType();
    }

    private String stepName(PipelineTemplateConfig config, int index, PipelineStepModel fallback) {
        if (index >= 0 && index < config.steps().size() && config.steps().get(index) != null) {
            return config.steps().get(index).name();
        }
        return fallback.serviceName();
    }
}
