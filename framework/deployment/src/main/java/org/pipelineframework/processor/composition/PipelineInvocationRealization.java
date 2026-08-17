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

/**
 * Placement-specific realization of a compiler-resolved pipeline invocation.
 *
 * <p>{@link PipelineInvocationBinding} remains the placement-neutral semantic contract. A local
 * realization can create a step adapter today; a future remote realization can create a compatible
 * boundary adapter from the same binding without changing the linked pipeline definition.
 *
 * @param <T> realized invocation representation
 */
@FunctionalInterface
public interface PipelineInvocationRealization<T> {

    /**
     * Realizes one statically linked invocation for a placement implementation.
     *
     * @param binding placement-neutral semantic invocation binding
     * @return placement-specific invocation representation
     */
    T realize(PipelineInvocationBinding binding);
}
