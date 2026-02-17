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

import javax.annotation.Nonnull;

/**
 * Interface for mapping between application domain types and external library entity types.
 * This mapper is used when a pipeline step delegates to an external library service
 * and needs to transform between the application's domain types and the library's entity types.
 *
 * @param <TApplicationInput> The application's input domain type
 * @param <TLibraryInput> The library's input entity type
 * @param <TApplicationOutput> The application's output domain type
 * @param <TLibraryOutput> The library's output entity type
 */
public interface ExternalMapper<TApplicationInput, TLibraryInput, TApplicationOutput, TLibraryOutput> {

    /**
     * Maps from the application's input domain type to the library's input entity type.
     *
     * @param applicationInput The input in the application's domain type (must not be null)
     * @return The mapped input in the library's entity type (must not be null)
     * @throws IllegalArgumentException if applicationInput is null
     */
    @Nonnull TLibraryInput toLibraryInput(@Nonnull TApplicationInput applicationInput);

    /**
     * Maps from the library's output entity type to the application's output domain type.
     *
     * @param libraryOutput The output in the library's entity type (must not be null)
     * @return The mapped output in the application's domain type (must not be null)
     * @throws IllegalArgumentException if libraryOutput is null
     */
    @Nonnull TApplicationOutput toApplicationOutput(@Nonnull TLibraryOutput libraryOutput);
}