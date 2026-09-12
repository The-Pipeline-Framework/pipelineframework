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

import io.smallrye.mutiny.Uni;

/**
 * Functional interface for reactive services that process an input object asynchronously
 * and return a result in a reactive manner using Mutiny Uni.
 * 
 * @param <T> the type of input object to process
 * @param <S> the type of output/result object
 */
@FunctionalInterface
public interface ReactiveService<T, S> {
  /**
   * Process the input object asynchronously and return a Uni containing the result.
   * 
   * @param processableObj the input object to process
   * @return a Uni that emits the processed result
   */
  Uni<S> process(T processableObj);
}
