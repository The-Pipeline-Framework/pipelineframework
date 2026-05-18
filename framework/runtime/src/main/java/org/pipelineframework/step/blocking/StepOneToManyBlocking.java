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
import org.pipelineframework.step.StepOneToMany;

public interface StepOneToManyBlocking<I, O> extends StepOneToMany<I, O> {

    /**
     * Materializes the full output list before any downstream item is emitted.
     *
     * <p>Prefer {@link StepOneToManyBlockingIterator} when a blocking library can iterate results
     * incrementally and you want to avoid full in-memory materialization.
     */
    List<O> applyBlocking(I in);

    @Override
    default Multi<O> applyOneToMany(I in) {
        return BlockingExecutions.emitList(this, () -> applyBlocking(in));
    }
}
