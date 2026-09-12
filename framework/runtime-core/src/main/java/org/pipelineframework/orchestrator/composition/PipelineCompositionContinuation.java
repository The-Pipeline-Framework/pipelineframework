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

/** Compiler-derived continuation for a definition-local node. */
public record PipelineCompositionContinuation(
    String nodeId,
    PipelineCompositionContinuationKind kind,
    String nextNodeId
) {
    public PipelineCompositionContinuation {
        nodeId = required(nodeId, "nodeId");
        kind = Objects.requireNonNull(kind, "kind must not be null");
        nextNodeId = nextNodeId == null ? "" : nextNodeId.strip();
        if (kind == PipelineCompositionContinuationKind.NEXT_LOCAL && nextNodeId.isEmpty()) {
            throw new IllegalArgumentException("NEXT_LOCAL continuation requires nextNodeId");
        }
        if (kind != PipelineCompositionContinuationKind.NEXT_LOCAL && !nextNodeId.isEmpty()) {
            throw new IllegalArgumentException(kind + " continuation must not declare nextNodeId");
        }
    }

    private static String required(String value, String field) {
        String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}
