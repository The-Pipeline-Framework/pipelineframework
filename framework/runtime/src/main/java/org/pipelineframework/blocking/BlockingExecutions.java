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
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import org.pipelineframework.annotation.PipelineStep;

/**
 * Runtime access helpers for blocking step default adapters.
 */
public final class BlockingExecutions {

    private static final BlockingExecutionSupport FALLBACK_SUPPORT = new BlockingExecutionSupport();

    private BlockingExecutions() {
    }

    public static <T> Uni<T> supply(Object owner, Supplier<T> supplier) {
        return support().supply(useVirtualThreads(owner), supplier);
    }

    public static <T> Multi<T> emitList(Object owner, Supplier<List<T>> supplier) {
        return support().emitList(useVirtualThreads(owner), supplier);
    }

    public static <T> Multi<T> emitIterator(Object owner, Supplier<? extends CloseableIterator<T>> supplier) {
        return support().emitIterator(useVirtualThreads(owner), supplier);
    }

    public static boolean useVirtualThreads(Object owner) {
        if (owner == null) {
            return false;
        }
        PipelineStep annotation = owner.getClass().getAnnotation(PipelineStep.class);
        return annotation != null && annotation.runOnVirtualThreads();
    }

    private static BlockingExecutionSupport support() {
        try {
            CDI<Object> cdi = CDI.current();
            Instance<BlockingExecutionSupport> instance = cdi.select(BlockingExecutionSupport.class);
            if (!instance.isUnsatisfied()) {
                return instance.get();
            }
        } catch (RuntimeException ignored) {
            // Tests and non-CDI callers fall back to the local support instance.
        }
        return FALLBACK_SUPPORT;
    }
}
