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

import java.util.List;
import java.util.Objects;

/**
 * Root-relative static location derived when a definition graph is linked.
 */
public record CompiledPipelineLocation(
    List<DefinitionLocalLocation> invocationPath,
    DefinitionLocalLocation definitionLocalLocation
) {

    public CompiledPipelineLocation {
        invocationPath = List.copyOf(Objects.requireNonNull(invocationPath, "invocationPath must not be null"));
        definitionLocalLocation = Objects.requireNonNull(
            definitionLocalLocation,
            "definitionLocalLocation must not be null");
    }

    /**
     * Human-readable root-relative form for diagnostics and proof metadata.
     *
     * @return display form preserving every invoking callsite
     */
    public String display() {
        StringBuilder value = new StringBuilder();
        for (DefinitionLocalLocation invocation : invocationPath) {
            append(value, invocation);
        }
        append(value, definitionLocalLocation);
        return value.toString();
    }

    private static void append(StringBuilder value, DefinitionLocalLocation location) {
        if (!value.isEmpty()) {
            value.append('/');
        }
        value.append(location.definition().logicalId()).append(':').append(location.localStepId());
    }
}
