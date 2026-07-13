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

package org.pipelineframework.config.template;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Normalizes the logical contracts of an explicitly linear v2 pipeline.
 */
public final class PipelineTemplateContractNormalizer {

    /**
     * Propagate a scalar pipeline input through omitted step inputs and validate an optional final output assertion.
     */
    public List<PipelineTemplateStep> normalize(
        String inputContract,
        String outputContract,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateMessage> messages,
        Map<String, PipelineTemplateUnion> unions
    ) {
        List<PipelineTemplateStep> authoredSteps = steps == null ? List.of() : steps;
        if (inputContract == null || inputContract.isBlank()) {
            validateOutputAssertion(outputContract, authoredSteps, messages);
            return authoredSteps;
        }
        requireConcreteContract(inputContract, "Pipeline input", messages, unions);
        if (isBranchAware(authoredSteps, unions)) {
            throw new IllegalStateException(
                "Linear input propagation cannot be used with branch-aware templates; declare every step input explicitly.");
        }

        List<PipelineTemplateStep> normalized = new ArrayList<>(authoredSteps.size());
        PipelineTemplateStep previous = null;
        for (PipelineTemplateStep step : authoredSteps) {
            if (step == null) {
                normalized.add(null);
                continue;
            }
            String inherited = previous == null ? inputContract : previous.outputTypeName();
            if (inherited == null || inherited.isBlank() || unions.containsKey(inherited)) {
                throw new IllegalStateException("Cannot inherit input for step '" + step.name() + "' from '"
                    + (previous == null ? "pipeline input" : previous.name())
                    + "': the previous output is missing or not singular; declare " + step.name() + ".input explicitly.");
            }
            requireConcreteContract(inherited, previous == null ? "Pipeline input" : "Previous step output", messages, unions);

            String explicit = normalize(step.inputTypeName());
            if (explicit != null && !explicit.equals(inherited)) {
                if (previous == null) {
                    throw new IllegalStateException("Step '" + step.name() + "' declares input '" + explicit
                        + "', but pipeline input resolves to '" + inherited + "'.");
                }
                throw new IllegalStateException("Step '" + step.name() + "' declares input '" + explicit
                    + "', but previous step '" + previous.name() + "' resolves output '" + inherited + "'.");
            }
            PipelineTemplateStep resolved = explicit == null
                ? step.withInputContract(inherited, step.inputFields())
                : step;
            normalized.add(resolved);
            previous = resolved;
        }
        List<PipelineTemplateStep> result = Collections.unmodifiableList(new ArrayList<>(normalized));
        validateOutputAssertion(outputContract, result, messages);
        return result;
    }

    private void validateOutputAssertion(
        String outputContract,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateMessage> messages
    ) {
        String asserted = normalize(outputContract);
        if (asserted == null) {
            return;
        }
        if (!messages.containsKey(asserted)) {
            throw new IllegalStateException("Pipeline output contract '" + asserted + "' must reference a declared concrete type");
        }
        PipelineTemplateStep last = null;
        for (PipelineTemplateStep step : steps) {
            if (step != null) {
                last = step;
            }
        }
        String resolved = last == null ? null : normalize(last.outputTypeName());
        if (!asserted.equals(resolved)) {
            throw new IllegalStateException("Pipeline output assertion '" + asserted + "' does not match final step '"
                + (last == null ? "<none>" : last.name()) + "' output '" + resolved + "'.");
        }
    }

    private void requireConcreteContract(
        String contract,
        String owner,
        Map<String, PipelineTemplateMessage> messages,
        Map<String, PipelineTemplateUnion> unions
    ) {
        if (unions.containsKey(contract)) {
            throw new IllegalStateException(owner + " contract '" + contract + "' is not singular; declare a concrete type");
        }
        if (!messages.containsKey(contract)) {
            throw new IllegalStateException(owner + " contract '" + contract + "' must reference a declared concrete type");
        }
    }

    private boolean isBranchAware(List<PipelineTemplateStep> steps, Map<String, PipelineTemplateUnion> unions) {
        return steps.stream()
            .filter(java.util.Objects::nonNull)
            .anyMatch(step -> step.terminal()
                || !step.accepts().isEmpty()
                || isUnion(step.inputTypeName(), unions)
                || isUnion(step.outputTypeName(), unions));
    }

    private boolean isUnion(String contract, Map<String, PipelineTemplateUnion> unions) {
        return contract != null && unions.containsKey(contract);
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
