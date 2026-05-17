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

package org.pipelineframework.service.blocking;

import java.util.List;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.blocking.BlockingExecutions;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;

@FunctionalInterface
public interface BlockingBidirectionalStreamingService<T, S> extends ReactiveBidirectionalStreamingService<T, S> {

    /**
     * Materializes the full input list and the full output list.
     *
     * <p>This contract is intended for bounded batch-style transformations. It does not preserve
     * incremental stream semantics, and batch retries rerun the full callback.
     */
    List<S> processBlocking(List<T> processableObj);

    @Override
    default Multi<S> process(Multi<T> processableObj) {
        return processableObj.collect()
            .asList()
            .onItem()
            .transformToMulti(items -> BlockingExecutions.emitList(this, () -> processBlocking(items)));
    }
}
