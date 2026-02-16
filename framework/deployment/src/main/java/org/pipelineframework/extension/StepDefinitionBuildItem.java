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

import io.quarkus.builder.item.MultiBuildItem;
import io.smallrye.common.annotation.Experimental;
import org.jboss.jandex.DotName;

import java.util.Objects;

/**
 * Build item carrying step definition metadata from the annotation processor
 * to the Quarkus build step phase.
 * <p>
 * This build item contains only type names (DotNames), not Class instances,
 * to avoid runtime class loading. The actual mapper resolution happens
 * in the build step using the Jandex index.
 */
@Experimental("Mapper inference based on Jandex index")
public final class StepDefinitionBuildItem extends MultiBuildItem {

    private final String stepName;
    private final DotName domainIn;
    private final DotName domainOut;
    private final String stepCardinality;

    /**
     * Creates a new step definition build item.
     *
     * @param stepName the fully qualified name of the step service class
     * @param domainIn DotName of the input domain type (E_in), or null if not specified
     * @param domainOut DotName of the output domain type (E_out), or null if not specified
     * @param stepCardinality the cardinality of the step; an empty string is accepted as a sentinel
     *                       value produced by MapperInferenceBuildSteps.readStepDefinitions() when
     *                       cardinality is not explicitly specified
     */
    public StepDefinitionBuildItem(String stepName, DotName domainIn, DotName domainOut, String stepCardinality) {
        this.stepName = Objects.requireNonNull(stepName, "stepName must not be null");
        this.stepCardinality = Objects.requireNonNull(stepCardinality, "stepCardinality must not be null");
        if (this.stepName.isBlank()) {
            throw new IllegalArgumentException("stepName must not be blank");
        }
        // stepCardinality is allowed to be empty as a sentinel value from readStepDefinitions()
        this.domainIn = domainIn;
        this.domainOut = domainOut;
    }

    /**
     * Gets the fully qualified name of the step service class.
     *
     * @return the step class name
     */
    public String getStepName() {
        return stepName;
    }

    /**
     * Gets the DotName of the input domain type (E_in).
     * <p>
     * This is the entity type that flows into the step, used to resolve
     * the outbound mapper: {@code Mapper<?, ?, E_in>}.
     *
     * @return the input domain type DotName, or null if not specified
     */
    public DotName getDomainIn() {
        return domainIn;
    }

    /**
     * Gets the DotName of the output domain type (E_out).
     * <p>
     * This is the entity type that flows out of the step, used to resolve
     * the inbound mapper: {@code Mapper<?, ?, E_out>}.
     *
     * @return the output domain type DotName, or null if not specified
     */
    public DotName getDomainOut() {
        return domainOut;
    }

    /**
     * Gets the step cardinality.
     *
     * @return the cardinality string (e.g., "ONE_TO_ONE", "ONE_TO_MANY")
     */
    public String getStepCardinality() {
        return stepCardinality;
    }

    @Override
    public String toString() {
        return "StepDefinitionBuildItem{" +
                "stepName='" + stepName + '\'' +
                ", domainIn=" + domainIn +
                ", domainOut=" + domainOut +
                ", stepCardinality='" + stepCardinality + '\'' +
                '}';
    }
}
