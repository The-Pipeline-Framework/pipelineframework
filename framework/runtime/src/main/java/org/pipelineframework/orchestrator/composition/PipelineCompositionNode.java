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

package org.pipelineframework.orchestrator.composition;

import java.util.Objects;
import java.util.List;

/** One ordered node in a resolved definition. */
public record PipelineCompositionNode(
    int index,
    String nodeId,
    String kind,
    String inputContractId,
    String outputContractId,
    String cardinality,
    String targetDefinitionId,
    List<String> acceptedContractIds,
    boolean terminal
) {
    public static final String DIRECT = "direct";
    public static final String INVOCATION = "pipeline";

    public PipelineCompositionNode {
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }
        nodeId = required(nodeId, "nodeId");
        kind = required(kind, "kind");
        inputContractId = required(inputContractId, "inputContractId");
        outputContractId = required(outputContractId, "outputContractId");
        cardinality = required(cardinality, "cardinality");
        targetDefinitionId = targetDefinitionId == null ? "" : targetDefinitionId.strip();
        acceptedContractIds = acceptedContractIds == null ? List.of() : acceptedContractIds.stream()
            .map(value -> required(value, "acceptedContractIds entry"))
            .distinct()
            .toList();
        if (!DIRECT.equals(kind) && !INVOCATION.equals(kind)) {
            throw new IllegalArgumentException("Unsupported composition node kind: " + kind);
        }
        if (DIRECT.equals(kind) && !targetDefinitionId.isEmpty()) {
            throw new IllegalArgumentException("Direct composition nodes must not declare targetDefinitionId");
        }
        if (INVOCATION.equals(kind) && targetDefinitionId.isEmpty()) {
            throw new IllegalArgumentException("Pipeline invocation nodes must declare targetDefinitionId");
        }
    }

    public PipelineCompositionNode(
        int index,
        String nodeId,
        String kind,
        String inputContractId,
        String outputContractId,
        String cardinality,
        String targetDefinitionId
    ) {
        this(index, nodeId, kind, inputContractId, outputContractId, cardinality, targetDefinitionId, List.of(), false);
    }

    public boolean invocation() {
        return INVOCATION.equals(kind);
    }

    private static String required(String value, String field) {
        String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}
