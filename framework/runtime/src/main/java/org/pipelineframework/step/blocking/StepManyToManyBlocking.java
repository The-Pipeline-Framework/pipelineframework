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

import java.util.List;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.blocking.BlockingExecutions;
import org.pipelineframework.step.StepManyToMany;

public interface StepManyToManyBlocking<I, O> extends StepManyToMany<I, O> {

    /**
     * Materializes the full input list and the full output list.
     *
     * <p>This is a bounded batch contract. It does not preserve incremental stream semantics.
     * Inputs and outputs are fully resident in memory during execution, so large batches can
     * increase heap pressure and GC cost. Prefer explicit chunking, reactive backpressure, or an
     * incremental streaming contract when batch size is large or unbounded.
     */
    List<O> applyBatchBlocking(List<I> inputs);

    @Override
    default Multi<O> applyTransform(Multi<I> input) {
        return input.collect().asList()
            .onItem().transformToMulti(items -> BlockingExecutions.emitList(this, () -> applyBatchBlocking(items)));
    }
}
