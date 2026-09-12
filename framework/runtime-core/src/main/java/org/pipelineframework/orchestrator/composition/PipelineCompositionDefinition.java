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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable resolved definition embedded in a pinned pipeline contract. */
public record PipelineCompositionDefinition(
    String definitionId,
    String definitionFingerprint,
    String inputContractId,
    String outputContractId,
    List<PipelineCompositionNode> nodes,
    List<PipelineCompositionContinuation> continuations
) {
    public PipelineCompositionDefinition {
        definitionId = required(definitionId, "definitionId");
        definitionFingerprint = required(definitionFingerprint, "definitionFingerprint");
        inputContractId = required(inputContractId, "inputContractId");
        outputContractId = required(outputContractId, "outputContractId");
        nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes must not be null"));
        continuations = List.copyOf(Objects.requireNonNull(continuations, "continuations must not be null"));
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("definition must contain nodes");
        }
        if (nodes.size() != continuations.size()) {
            throw new IllegalArgumentException("definition nodes and continuations must have the same size");
        }
        Set<String> nodeIds = new HashSet<>();
        Set<String> allNodeIds = nodes.stream()
            .map(PipelineCompositionNode::nodeId)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
        for (int index = 0; index < nodes.size(); index++) {
            PipelineCompositionNode node = Objects.requireNonNull(nodes.get(index), "nodes must not contain null");
            if (node.index() != index || !nodeIds.add(node.nodeId())) {
                throw new IllegalArgumentException("definition nodes must have unique contiguous indexes and ids");
            }
            PipelineCompositionContinuation continuation = Objects.requireNonNull(
                continuations.get(index), "continuations must not contain null");
            if (!node.nodeId().equals(continuation.nodeId())) {
                throw new IllegalArgumentException("continuation must match its definition node");
            }
            if (continuation.kind() == PipelineCompositionContinuationKind.NEXT_LOCAL
                && !allNodeIds.contains(continuation.nextNodeId())) {
                throw new IllegalArgumentException("continuation nextNodeId is not a definition node");
            }
            if (index + 1 < nodes.size()) {
                if (continuation.kind() != PipelineCompositionContinuationKind.NEXT_LOCAL
                    || !nodes.get(index + 1).nodeId().equals(continuation.nextNodeId())) {
                    throw new IllegalArgumentException("non-terminal definition nodes must continue to the next local node");
                }
            } else if (continuation.kind() == PipelineCompositionContinuationKind.NEXT_LOCAL) {
                throw new IllegalArgumentException("terminal definition node cannot have NEXT_LOCAL continuation");
            }
        }
    }

    public PipelineCompositionNode node(String nodeId) {
        return nodes.stream().filter(node -> node.nodeId().equals(nodeId)).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown definition node " + definitionId + ":" + nodeId));
    }

    public PipelineCompositionContinuation continuation(String nodeId) {
        return continuations.stream().filter(value -> value.nodeId().equals(nodeId)).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown continuation " + definitionId + ":" + nodeId));
    }

    private static String required(String value, String field) {
        String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}
