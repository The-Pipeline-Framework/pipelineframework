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

import java.util.Map;
import java.util.Optional;

/**
 * Interprets v2 step logical contracts and Java execution bindings consistently.
 */
public final class PipelineTemplateStepContractSyntax {

    private PipelineTemplateStepContractSyntax() {
    }

    public static StepContracts normalize(Map<?, ?> step, int version, String stepName) {
        Optional<String> canonicalInput = stringValue(step, "input", stepName);
        Optional<String> canonicalOutput = stringValue(step, "output", stepName);
        Optional<String> legacyInput = stringValue(step, "inputTypeName", stepName);
        Optional<String> legacyOutput = stringValue(step, "outputTypeName", stepName);

        if (version < 2) {
            if (step.containsKey("java")) {
                throw new IllegalStateException("Step '" + stepName + "' java bindings require version: 2.");
            }
            return new StepContracts(legacyInput, legacyOutput,
                canonicalInput.isPresent() ? canonicalInput : legacyInput,
                canonicalOutput.isPresent() ? canonicalOutput : legacyOutput,
                false);
        }

        JavaContracts java = readJava(step, stepName);
        Direction input = normalizeDirection("input", canonicalInput, legacyInput, java.input(), stepName);
        Direction output = normalizeDirection("output", canonicalOutput, legacyOutput, java.output(), stepName);
        return new StepContracts(input.logical(), output.logical(), input.javaType(), output.javaType(),
            input.legacyFqcn() || output.legacyFqcn());
    }

    private static Direction normalizeDirection(
        String direction,
        Optional<String> canonical,
        Optional<String> legacy,
        Optional<String> explicitJava,
        String stepName
    ) {
        if (canonical.filter(PipelineTemplateStepContractSyntax::isLogicalName).isPresent()) {
            if (legacy.isPresent() && !legacy.equals(canonical)) {
                throw new IllegalStateException("Step '" + stepName + "' declares conflicting logical " + direction
                    + " contracts in '" + direction + "' ('" + canonical + "') and '" + direction
                    + "TypeName' ('" + legacy + "').");
            }
            return new Direction(canonical, explicitJava, false);
        }
        if (canonical.isPresent()) {
            if (explicitJava.isPresent() && !explicitJava.equals(canonical)) {
                throw new IllegalStateException("Step '" + stepName + "' declares conflicting Java " + direction
                    + " contracts in legacy '" + direction + "' ('" + canonical + "') and 'java."
                    + direction + "' ('" + explicitJava + "').");
            }
            return new Direction(legacy, canonical, true);
        }
        if (legacy.filter(value -> !isLogicalName(value)).isPresent()) {
            return new Direction(Optional.empty(), legacy, true);
        }
        return new Direction(legacy, explicitJava, false);
    }

    private static JavaContracts readJava(Map<?, ?> step, String stepName) {
        Object java = step.get("java");
        if (java == null) {
            return new JavaContracts(Optional.empty(), Optional.empty());
        }
        if (!(java instanceof Map<?, ?> javaMap)) {
            throw new IllegalStateException("Step '" + stepName + "' java binding must be a YAML map.");
        }
        return new JavaContracts(stringValue(javaMap, "input", stepName),
            stringValue(javaMap, "output", stepName));
    }

    private static Optional<String> stringValue(Map<?, ?> values, String key, String stepName) {
        Object value = values.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (!(value instanceof String text)) {
            throw new IllegalStateException("Step '" + stepName + "' " + key + " contract must be a YAML string.");
        }
        text = text.trim();
        return text.isEmpty() ? Optional.empty() : Optional.of(text);
    }

    public static boolean isLogicalName(String value) {
        return value != null && !value.isBlank() && !value.contains(".");
    }

    public record StepContracts(
        Optional<String> logicalInput,
        Optional<String> logicalOutput,
        Optional<String> javaInput,
        Optional<String> javaOutput,
        boolean usesLegacyFqcn
    ) {
    }

    private record Direction(Optional<String> logical, Optional<String> javaType, boolean legacyFqcn) {
    }

    private record JavaContracts(Optional<String> input, Optional<String> output) {
    }
}
