/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework.mapper;

import jakarta.annotation.Nonnull;

/**
 * Interface for mapping between application domain types and operator entity types.
 * This mapper is used when a pipeline step delegates to an operator service
 * and needs to transform between the application's domain types and the operator's entity types.
 *
 * @param <TApplicationInput> The application's input domain type
 * @param <TOperatorInput> The operator's input entity type
 * @param <TApplicationOutput> The application's output domain type
 * @param <TOperatorOutput> The operator's output entity type
 */
public interface ExternalMapper<TApplicationInput, TOperatorInput, TApplicationOutput, TOperatorOutput> {

    /**
         * Map an application-domain input to the operator's input entity type.
         *
         * @param applicationInput the application input; must not be null
         * @return the corresponding operator input; never null
         * @throws IllegalArgumentException if {@code applicationInput} is null
         */
    @Nonnull
    TOperatorInput toOperatorInput(@Nonnull TApplicationInput applicationInput);

    /**
     * Converts application input to the operator's input type; retained for backward compatibility.
     *
     * @param applicationInput the input in the application's domain type; must not be null
     * @return the mapped input in the operator's entity type; never null
     * @throws IllegalArgumentException if {@code applicationInput} is null
     * @deprecated Use {@link #toOperatorInput(Object)}.
     */
    @Deprecated
    @Nonnull
    default TOperatorInput toLibraryInput(@Nonnull TApplicationInput applicationInput) {
        return toOperatorInput(applicationInput);
    }

    /**
     * Maps from the operator's output entity type to the application's output domain type.
     *
     * @param operatorOutput The output in the operator's entity type (must not be null)
     * @return The mapped output in the application's domain type (must not be null)
     * @throws IllegalArgumentException if operatorOutput is null
     */
    @Nonnull TApplicationOutput toApplicationOutput(@Nonnull TOperatorOutput operatorOutput);
}
