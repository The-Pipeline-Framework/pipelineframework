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

/**
 * Interprets v2 step logical contracts and Java execution bindings consistently.
 */
public final class PipelineTemplateStepContractSyntax {

    private PipelineTemplateStepContractSyntax() {
    }

    public static StepContracts normalize(Map<?, ?> step, int version, String stepName) {
        String canonicalInput = stringValue(step, "input");
        String canonicalOutput = stringValue(step, "output");
        String legacyInput = stringValue(step, "inputTypeName");
        String legacyOutput = stringValue(step, "outputTypeName");
        JavaContracts java = readJava(step, stepName);

        if (version < 2) {
            return new StepContracts(legacyInput, legacyOutput,
                canonicalInput != null ? canonicalInput : legacyInput,
                canonicalOutput != null ? canonicalOutput : legacyOutput,
                false);
        }

        Direction input = normalizeDirection("input", canonicalInput, legacyInput, java.input(), stepName);
        Direction output = normalizeDirection("output", canonicalOutput, legacyOutput, java.output(), stepName);
        return new StepContracts(input.logical(), output.logical(), input.javaType(), output.javaType(),
            input.legacyFqcn() || output.legacyFqcn());
    }

    private static Direction normalizeDirection(
        String direction,
        String canonical,
        String legacy,
        String explicitJava,
        String stepName
    ) {
        if (isLogicalName(canonical)) {
            if (legacy != null && !legacy.equals(canonical)) {
                throw new IllegalStateException("Step '" + stepName + "' declares conflicting logical " + direction
                    + " contracts in '" + direction + "' ('" + canonical + "') and '" + direction
                    + "TypeName' ('" + legacy + "').");
            }
            return new Direction(canonical, explicitJava, false);
        }
        if (canonical != null) {
            if (explicitJava != null && !explicitJava.equals(canonical)) {
                throw new IllegalStateException("Step '" + stepName + "' declares conflicting Java " + direction
                    + " contracts in legacy '" + direction + "' ('" + canonical + "') and 'java."
                    + direction + "' ('" + explicitJava + "').");
            }
            return new Direction(legacy, canonical, true);
        }
        if (legacy != null && !isLogicalName(legacy)) {
            return new Direction(null, legacy, true);
        }
        return new Direction(legacy, explicitJava, false);
    }

    private static JavaContracts readJava(Map<?, ?> step, String stepName) {
        Object java = step.get("java");
        if (java == null) {
            return new JavaContracts(null, null);
        }
        if (!(java instanceof Map<?, ?> javaMap)) {
            throw new IllegalStateException("Step '" + stepName + "' java binding must be a YAML map.");
        }
        return new JavaContracts(stringValue(javaMap, "input"), stringValue(javaMap, "output"));
    }

    private static String stringValue(Map<?, ?> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return null;
        }
        String text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    public static boolean isLogicalName(String value) {
        return value != null && !value.isBlank() && !value.contains(".");
    }

    public record StepContracts(
        String logicalInput,
        String logicalOutput,
        String javaInput,
        String javaOutput,
        boolean usesLegacyFqcn
    ) {
    }

    private record Direction(String logical, String javaType, boolean legacyFqcn) {
    }

    private record JavaContracts(String input, String output) {
    }
}
