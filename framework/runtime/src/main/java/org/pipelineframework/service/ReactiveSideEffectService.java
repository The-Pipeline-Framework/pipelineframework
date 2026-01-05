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

package org.pipelineframework.service;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Interface for reactive side effect services that process input and return the original input unchanged.
 * This is used for plugin implementations that perform side effects without transforming the data.
 *
 * @param <T> the type of input/output object
 */
public interface ReactiveSideEffectService<T> extends ReactiveService<T, T> {
    /**
     * Process a single input object and return the original input unchanged after performing side effects.
     *
     * @param input the input object to process
     * @return a Uni that emits the original input object unchanged
     */
    @Override
    Uni<T> process(T input);

    /**
     * Process a stream of input objects and return the same stream after performing side effects on each item.
     * This is a default implementation that uses the single-item method.
     *
     * @param input the stream of input objects to process
     * @return a Multi stream of the original input objects unchanged
     */
    default Multi<T> process(Multi<T> input) {
        return input.onItem().transformToUni(this::process).concatenate();
    }
}
