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
 * Interface for reactive streaming client services that process a stream of inputs and return a single output.
 *
 * @param <T> the input type
 * @param <S> the output type
 */
@FunctionalInterface
public interface ReactiveStreamingClientService<T, S> {
  /**
   * Process a stream of input objects and return a single output object.
   *
   * @param processableObj the stream of input objects to process
   * @return a Uni with the single output object
   */
  Uni<S> process(Multi<T> processableObj);
}
