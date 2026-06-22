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

package org.pipelineframework.blocking;

import java.util.List;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Runtime access helpers for blocking step default adapters.
 */
public final class BlockingExecutions {

    private static final BlockingExecutionSupport FALLBACK_SUPPORT = new BlockingExecutionSupport();

    private BlockingExecutions() {
    }

    /**
     * Offloads a blocking supplier on the default worker pool.
     *
     * @param owner retained for source compatibility with existing blocking service defaults; ignored
     * @param supplier blocking supplier to execute
     * @return a Uni backed by the offloaded supplier
     */
    public static <T> Uni<T> supply(Object owner, Supplier<T> supplier) {
        return support().supply(false, supplier);
    }

    /**
     * Offloads a blocking list supplier on the default worker pool.
     *
     * @param owner retained for source compatibility with existing blocking service defaults; ignored
     * @param supplier blocking supplier to execute
     * @return a Multi over the supplied list
     */
    public static <T> Multi<T> emitList(Object owner, Supplier<List<T>> supplier) {
        return support().emitList(false, supplier);
    }

    /**
     * Offloads blocking iterator acquisition and iteration on the default worker pool.
     *
     * @param owner retained for source compatibility with existing blocking service defaults; ignored
     * @param supplier blocking iterator supplier to execute
     * @return a Multi over the iterator items
     */
    public static <T> Multi<T> emitIterator(Object owner, Supplier<? extends CloseableIterator<T>> supplier) {
        return support().emitIterator(false, supplier);
    }

    private static BlockingExecutionSupport support() {
        try {
            return RuntimeAdapters.resolveBean(BlockingExecutionSupport.class)
                .orElse(FALLBACK_SUPPORT);
        } catch (RuntimeException ignored) {
            // Tests and non-CDI callers fall back to the local support instance.
        }
        return FALLBACK_SUPPORT;
    }
}
