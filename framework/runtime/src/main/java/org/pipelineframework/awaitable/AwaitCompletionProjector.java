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

package org.pipelineframework.awaitable;

/**
 * Pure application boundary that combines the canonical request suspended by an Await step with
 * the actor's admitted completion payload.
 *
 * <p>Implementations must be deterministic and side-effect free. The runtime invokes the
 * projector before persisting the canonical completion, so replay never needs to call it again.</p>
 *
 * @param <I> canonical Await request type
 * @param <C> admitted completion payload type
 * @param <O> canonical Await output type
 */
@FunctionalInterface
public interface AwaitCompletionProjector<I, C, O> {

    O project(I request, C completion, AwaitCompletionMetadata metadata);
}
