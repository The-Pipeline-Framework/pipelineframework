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

import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.config.CardinalitySemantics;

/**
 * Compiler-owned structural step in a reusable pipeline definition.
 *
 * <p>This proof model deliberately represents only semantic contracts and static references. It
 * does not model a runtime step implementation or introduce the DSL {@code pipeline} step kind.
 */
public record PipelineDefinitionStep(
    String localStepId,
    String inputContractId,
    String outputContractId,
    Optional<CardinalitySemantics> directCardinality,
    Optional<PipelineReference> pipelineReference
) {

    public PipelineDefinitionStep {
        localStepId = requireNonBlank(localStepId, "localStepId");
        inputContractId = requireNonBlank(inputContractId, "inputContractId");
        outputContractId = requireNonBlank(outputContractId, "outputContractId");
        directCardinality = Objects.requireNonNull(directCardinality, "directCardinality must not be null");
        pipelineReference = Objects.requireNonNull(pipelineReference, "pipelineReference must not be null");
        if (directCardinality.isPresent() == pipelineReference.isPresent()) {
            throw new IllegalArgumentException(
                "A definition step must declare exactly one of directCardinality or pipelineReference");
        }
    }

    public static PipelineDefinitionStep direct(
        String localStepId,
        String inputContractId,
        String outputContractId,
        CardinalitySemantics cardinality
    ) {
        return new PipelineDefinitionStep(
            localStepId,
            inputContractId,
            outputContractId,
            Optional.of(Objects.requireNonNull(cardinality, "cardinality must not be null")),
            Optional.empty());
    }

    public static PipelineDefinitionStep pipeline(
        String localStepId,
        String inputContractId,
        String outputContractId,
        PipelineReference pipelineReference
    ) {
        return new PipelineDefinitionStep(
            localStepId,
            inputContractId,
            outputContractId,
            Optional.empty(),
            Optional.of(Objects.requireNonNull(pipelineReference, "pipelineReference must not be null")));
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
