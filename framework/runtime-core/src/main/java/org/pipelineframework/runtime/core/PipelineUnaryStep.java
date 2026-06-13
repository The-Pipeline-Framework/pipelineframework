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

package org.pipelineframework.runtime.core;

import java.util.concurrent.CompletionStage;

/**
 * Framework-neutral unary pipeline step contract used by non-Quarkus runtime adapters.
 *
 * @param <I> input type
 * @param <O> output type
 */
public interface PipelineUnaryStep<I, O> {

    /**
     * Apply this unary step to one input item.
     *
     * @param input input item
     * @return stage that completes with the transformed output item
     */
    CompletionStage<O> apply(I input);
}
