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

import io.smallrye.mutiny.Uni;
import org.pipelineframework.blocking.BlockingExecutions;
import org.pipelineframework.service.ReactiveService;

@FunctionalInterface
public interface BlockingService<T, S> extends ReactiveService<T, S> {

    S processBlocking(T processableObj);

    @Override
    default Uni<S> process(T processableObj) {
        return BlockingExecutions.supply(this, () -> processBlocking(processableObj));
    }
}
