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

/** Canonical static location for one node within an acyclic composed pipeline. */
public record PipelineStaticLocation(List<NodeRef> invocationPath, NodeRef current) {
    public PipelineStaticLocation {
        invocationPath = List.copyOf(Objects.requireNonNull(invocationPath, "invocationPath must not be null"));
        invocationPath.forEach(frame -> Objects.requireNonNull(frame, "invocationPath must not contain null"));
        current = Objects.requireNonNull(current, "current must not be null");
    }

    public record NodeRef(String definitionId, String nodeId) {
        public NodeRef {
            definitionId = required(definitionId, "definitionId");
            nodeId = required(nodeId, "nodeId");
        }

        private static String required(String value, String field) {
            String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
            if (normalized.isEmpty()) {
                throw new IllegalArgumentException(field + " must not be blank");
            }
            return normalized;
        }
    }
}
