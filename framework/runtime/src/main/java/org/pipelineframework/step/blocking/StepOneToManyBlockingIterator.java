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

package org.pipelineframework.step.blocking;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.blocking.BlockingExecutions;
import org.pipelineframework.blocking.CloseableIterator;
import org.pipelineframework.step.StepOneToMany;

/**
 * Incremental blocking 1 -> N step contract.
 *
 * <p>Use this contract when authored code should stay synchronous but can produce results
 * incrementally via a cursor or iterator. The framework owns iterator closure when the step is
 * consumed through the pipeline runtime.
 */
public interface StepOneToManyBlockingIterator<I, O> extends StepOneToMany<I, O> {

    CloseableIterator<O> iterateBlocking(I in);

    @Override
    default Multi<O> applyOneToMany(I in) {
        return BlockingExecutions.emitIterator(this, () -> iterateBlocking(in));
    }
}
