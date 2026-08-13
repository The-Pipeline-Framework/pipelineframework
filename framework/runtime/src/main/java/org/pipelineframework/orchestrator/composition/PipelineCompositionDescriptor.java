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

import java.util.List;
import java.util.Objects;

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
        for (PipelineCompositionDefinition definition : definitions) {
            for (PipelineCompositionNode node : definition.nodes()) {
                if (node.invocation() && definitions.stream().noneMatch(
                    target -> target.definitionId().equals(node.targetDefinitionId()))) {
                    throw new IllegalArgumentException("composition invocation references an unknown definition: "
                        + node.targetDefinitionId());
                }
            }
            PipelineCompositionContinuation terminal = definition.continuations().getLast();
            if (definition.definitionId().equals(rootDefinitionId)
                != (terminal.kind() == PipelineCompositionContinuationKind.ROOT_TERMINAL)) {
                throw new IllegalArgumentException("only the root definition may have ROOT_TERMINAL continuation");
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
}
