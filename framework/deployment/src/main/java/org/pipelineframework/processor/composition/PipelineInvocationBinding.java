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
import org.pipelineframework.config.CardinalitySemantics;

/**
 * Compiler-resolved static invocation of a referenced pipeline definition.
 */
public record PipelineInvocationBinding(
    CompiledPipelineLocation invocationLocation,
    PipelineReference target,
    CardinalitySemantics cardinality
) {

    public PipelineInvocationBinding {
        invocationLocation = Objects.requireNonNull(invocationLocation, "invocationLocation must not be null");
        target = Objects.requireNonNull(target, "target must not be null");
        cardinality = Objects.requireNonNull(cardinality, "cardinality must not be null");
    }
}
