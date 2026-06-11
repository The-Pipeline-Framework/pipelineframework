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
import java.util.function.Supplier;

/**
 * Runtime-neutral abstraction for running potentially blocking work.
 */
public interface ReactiveRuntime {

    /**
     * Executes a supplier and returns a completion stage that captures the scheduled work.
     *
     * @param supplier task to execute
     * @param offloadToVirtualThread whether virtual-thread scheduling is desired
     * @param <T> result type
     * @return completion stage with the result
     */
    <T> CompletionStage<T> executeBlocking(Supplier<T> supplier, boolean offloadToVirtualThread);
}
