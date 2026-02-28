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

package org.pipelineframework.extension;

import io.quarkus.builder.item.SimpleBuildItem;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Build item that carries parsed pipeline step configuration from YAML.
 */
public final class PipelineConfigBuildItem extends SimpleBuildItem {

    private final List<StepConfig> steps;

    /**
     * Creates a PipelineConfigBuildItem containing the given pipeline step configurations.
     *
     * @param steps the list of StepConfig entries; must not be null and must not contain null elements
     * @throws NullPointerException if {@code steps} is null or contains a null element
     */
    public PipelineConfigBuildItem(List<StepConfig> steps) {
        Objects.requireNonNull(steps, "steps must not be null");
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i) == null) {
                throw new NullPointerException("steps contains null element at index " + i);
            }
        }
        this.steps = List.copyOf(steps);
    }

    /**
     * Provides the configured pipeline steps.
     *
     * @return an immutable list of StepConfig entries in declaration order
     */
    public List<StepConfig> steps() {
        return steps;
    }

    /**
     * Pipeline step configuration parsed from YAML.
     *
     * @param name step name in YAML
     * @param operator operator reference in "fully.qualified.Class::method" format
     */
    public record StepConfig(
            String name,
            String operator) {
        private static final Pattern QUALIFIED_CLASS_PATTERN = Pattern.compile(
                "[A-Za-z_$][A-Za-z\\d_$]*(\\.[A-Za-z_$][A-Za-z\\d_$]*)+");
        private static final Pattern METHOD_PATTERN = Pattern.compile("[A-Za-z_$][A-Za-z\\d_$]*");

        public StepConfig {
            name = Objects.requireNonNull(name, "step name must not be null").trim();
            operator = Objects.requireNonNull(operator, "step operator must not be null").trim();

            if (name.isBlank()) {
                throw new IllegalArgumentException("step name must not be blank");
            }
            if (operator.isBlank()) {
                throw new IllegalArgumentException("step operator must not be blank");
            }

            int separator = operator.indexOf("::");
            if (separator <= 0 || separator == operator.length() - 2 || operator.indexOf("::", separator + 2) != -1) {
                throw new IllegalArgumentException(
                        "step operator must contain a single '::' separator; got '" + operator + "'");
            }

            String classPart = operator.substring(0, separator);
            String methodPart = operator.substring(separator + 2);
            if (!QUALIFIED_CLASS_PATTERN.matcher(classPart).matches()
                    || !METHOD_PATTERN.matcher(methodPart).matches()) {
                throw new IllegalArgumentException(
                        "step operator has invalid class or method identifier; class='"
                                + classPart + "' method='" + methodPart + "'");
            }
        }
    }
}
