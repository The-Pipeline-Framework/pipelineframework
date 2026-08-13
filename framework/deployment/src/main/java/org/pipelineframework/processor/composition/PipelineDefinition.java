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

package org.pipelineframework.processor.composition;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Structural, source-neutral compiler definition of an ordered typed pipeline.
 *
 * <p>This class protects structural invariants only. The v3 compiler validates flow reachability,
 * union narrowing, and terminal coverage through its authoritative branch planner; imposing
 * adjacent input/output equality here would silently narrow that language.
 */
public record PipelineDefinition(
    PipelineReference reference,
    String inputContractId,
    String outputContractId,
    List<PipelineDefinitionStep> steps
) {

    public PipelineDefinition {
        reference = Objects.requireNonNull(reference, "reference must not be null");
        inputContractId = requireNonBlank(inputContractId, "inputContractId");
        outputContractId = requireNonBlank(outputContractId, "outputContractId");
        steps = List.copyOf(Objects.requireNonNull(steps, "steps must not be null"));
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("Pipeline definition must contain at least one step");
        }
        Set<String> localStepIds = new HashSet<>();
        for (int index = 0; index < steps.size(); index++) {
            PipelineDefinitionStep step = Objects.requireNonNull(steps.get(index), "steps must not contain null");
            if (!localStepIds.add(step.localStepId())) {
                throw new IllegalArgumentException("Duplicate definition-local step id: " + step.localStepId());
            }
        }
    }

    private static String requireNonBlank(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        String normalized = value.strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}
