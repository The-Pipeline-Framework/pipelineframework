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

/**
 * Interface for reactive streaming services that process a single input and return a stream of outputs.
 *
 * @param <T> the input type
 * @param <S> the output type
 */
public interface ReactiveStreamingService<T, S> {
  /**
   * Process a single input object and return a stream of output objects.
   *
   * @param processableObj the input object to process
   * @return a Multi stream of output objects
   */
  Multi<S> process(T processableObj);
}
