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

/**
 * Stable location owned by one reusable pipeline definition.
 */
public record DefinitionLocalLocation(PipelineReference definition, String localStepId) {

    public DefinitionLocalLocation {
        definition = Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(localStepId, "localStepId must not be null");
        localStepId = localStepId.strip();
        if (localStepId.isEmpty()) {
            throw new IllegalArgumentException("localStepId must not be blank");
        }
    }
}
