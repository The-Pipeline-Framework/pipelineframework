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
import io.smallrye.mutiny.Uni;
import org.pipelineframework.blocking.BlockingExecutions;
import org.pipelineframework.step.StepManyToOne;

public interface StepManyToOneBlocking<I, O> extends StepManyToOne<I, O> {

    /**
     * Materializes the full input list before invoking the blocking callback.
     *
     * <p>This is a bounded batch contract. Batch retries rerun the full callback with the collected
     * list.
     */
    O applyBatchBlocking(List<I> inputs);

    @Override
    default Uni<O> applyReduce(Multi<I> input) {
        return input.collect().asList()
            .onItem().transformToUni(items -> BlockingExecutions.supply(this, () -> applyBatchBlocking(items)));
    }
}
