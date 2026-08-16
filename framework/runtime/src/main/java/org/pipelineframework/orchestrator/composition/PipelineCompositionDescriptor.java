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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Release-pinned compiler projection of a resolved pipeline composition graph. */
public record PipelineCompositionDescriptor(String rootDefinitionId, List<PipelineCompositionDefinition> definitions) {
    public PipelineCompositionDescriptor {
        rootDefinitionId = rootDefinitionId == null ? "" : rootDefinitionId.strip();
        definitions = definitions == null ? List.of() : List.copyOf(definitions);
        if (rootDefinitionId.isEmpty() != definitions.isEmpty()) {
            throw new IllegalArgumentException("composition rootDefinitionId and definitions must be present together");
        }
        definitions.forEach(definition -> Objects.requireNonNull(definition, "composition definition must not be null"));
        String resolvedRootDefinitionId = rootDefinitionId;
        long roots = definitions.stream()
            .filter(definition -> definition.definitionId().equals(resolvedRootDefinitionId))
            .count();
        if (!definitions.isEmpty() && roots != 1L) {
            throw new IllegalArgumentException("composition must contain exactly one root definition");
        }
        long distinct = definitions.stream().map(PipelineCompositionDefinition::definitionId).distinct().count();
        if (distinct != definitions.size()) {
            throw new IllegalArgumentException("composition definitions must have unique definition ids");
        }
        Map<String, PipelineCompositionDefinition> definitionsById = new HashMap<>();
        definitions.forEach(definition -> definitionsById.put(definition.definitionId(), definition));
        for (PipelineCompositionDefinition definition : definitions) {
            for (PipelineCompositionNode node : definition.nodes()) {
                if (node.invocation() && !definitionsById.containsKey(node.targetDefinitionId())) {
                    throw new IllegalArgumentException("composition invocation references an unknown definition: "
                        + node.targetDefinitionId());
                }
            }
            PipelineCompositionContinuation terminal = definition.continuations().getLast();
            PipelineCompositionContinuationKind expectedTerminalKind = definition.definitionId().equals(rootDefinitionId)
                ? PipelineCompositionContinuationKind.ROOT_TERMINAL
                : PipelineCompositionContinuationKind.RETURN;
            if (terminal.kind() != expectedTerminalKind) {
                throw new IllegalArgumentException("composition definition '" + definition.definitionId()
                    + "' must end with " + expectedTerminalKind + " continuation");
            }
        }
        if (!definitions.isEmpty()) {
            Set<String> reached = new HashSet<>();
            visit(rootDefinitionId, definitionsById, reached, new HashSet<>());
            if (reached.size() != definitions.size()) {
                String unreachable = definitions.stream()
                    .map(PipelineCompositionDefinition::definitionId)
                    .filter(definitionId -> !reached.contains(definitionId))
                    .findFirst()
                    .orElseThrow();
                throw new IllegalArgumentException(
                    "composition definition is unreachable from root: " + unreachable);
            }
        }
    }

    public static PipelineCompositionDescriptor empty() {
        return new PipelineCompositionDescriptor("", List.of());
    }

    public boolean present() {
        return !definitions.isEmpty();
    }

    public PipelineCompositionDefinition definition(String definitionId) {
        Objects.requireNonNull(definitionId, "definitionId must not be null");
        return definitions.stream().filter(value -> value.definitionId().equals(definitionId)).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown composition definition: " + definitionId));
    }

    private static void visit(
        String definitionId,
        Map<String, PipelineCompositionDefinition> definitionsById,
        Set<String> reached,
        Set<String> visiting
    ) {
        if (reached.contains(definitionId)) {
            return;
        }
        if (!visiting.add(definitionId)) {
            throw new IllegalArgumentException("composition definitions must not contain cycles: " + definitionId);
        }
        PipelineCompositionDefinition definition = definitionsById.get(definitionId);
        definition.nodes().stream()
            .filter(PipelineCompositionNode::invocation)
            .map(PipelineCompositionNode::targetDefinitionId)
            .forEach(target -> visit(target, definitionsById, reached, visiting));
        visiting.remove(definitionId);
        reached.add(definitionId);
    }
}
