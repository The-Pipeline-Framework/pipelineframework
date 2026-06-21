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

import io.smallrye.mutiny.Multi;
import org.pipelineframework.blocking.BlockingExecutions;
import org.pipelineframework.blocking.CloseableIterator;
import org.pipelineframework.service.ReactiveStreamingService;

/**
 * Incremental blocking 1 -> N service contract.
 *
 * <p>Use this contract when a synchronous library can iterate results incrementally and you want
 * non-Mutiny authored code without forcing full in-memory materialization.
 *
 * <p>The returned iterator is still blocking. The default adapter offloads iterator acquisition
 * and item iteration to worker threads. Generated bridges may choose a different offload policy
 * from YAML-owned execution metadata.
 */
@FunctionalInterface
public interface BlockingIteratorService<T, S> extends ReactiveStreamingService<T, S> {

    CloseableIterator<S> iterateBlocking(T processableObj);

    @Override
    default Multi<S> process(T processableObj) {
        return BlockingExecutions.emitIterator(this, () -> iterateBlocking(processableObj));
    }
}
