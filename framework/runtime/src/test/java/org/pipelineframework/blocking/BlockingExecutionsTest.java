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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link BlockingExecutions} static helper methods.
 *
 * These tests verify that after the removal of the {@code useVirtualThreads(Object)} method and the
 * {@code @PipelineStep(runOnVirtualThreads)} attribute, all three entry points
 * ({@code supply}, {@code emitList}, {@code emitIterator}) unconditionally offload work to
 * worker (platform) threads, regardless of what owner object is passed.
 */
class BlockingExecutionsTest {

    // ---- supply ----

    @Test
    void supplyRunsOnWorkerThreadNotVirtualThread() {
        AtomicReference<Thread> executingThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();

        String result = BlockingExecutions.supply(this, () -> {
            executingThread.set(Thread.currentThread());
            return "done";
        }).await().atMost(Duration.ofSeconds(5));

        assertNotEquals(caller, executingThread.get(), "supply must not run on the caller thread");
        assertFalse(executingThread.get().isVirtual(), "supply must use a worker (platform) thread, not a virtual thread");
        assertEquals("done", result);
    }

    @Test
    void supplyWithNullOwnerRunsOnWorkerThread() {
        AtomicReference<Thread> executingThread = new AtomicReference<>();

        String result = BlockingExecutions.supply(null, () -> {
            executingThread.set(Thread.currentThread());
            return "null-owner-ok";
        }).await().atMost(Duration.ofSeconds(5));

        assertNotNull(executingThread.get(), "supply(null, ...) must still execute the supplier");
        assertFalse(executingThread.get().isVirtual(), "supply with null owner must use a worker thread");
        assertEquals("null-owner-ok", result);
    }

    /**
     * Regression test: the old {@code useVirtualThreads(Object)} helper read
     * {@code @PipelineStep(runOnVirtualThreads = true)} from the owner. That attribute was removed
     * from {@code @PipelineStep} in this PR. Even if someone passes an annotated owner object,
     * {@code supply} must still run on a worker thread.
     */
    @Test
    void supplyOwnerAnnotationDoesNotInfluenceThreadChoice() {
        // AnnotatedOwner does not have @PipelineStep at all after the annotation change;
        // we pass a plain object to confirm that no reflection-based virtual-thread selection occurs.
        Object ownerWithoutAnnotation = new Object();
        AtomicReference<Thread> executingThread = new AtomicReference<>();

        BlockingExecutions.supply(ownerWithoutAnnotation, () -> {
            executingThread.set(Thread.currentThread());
            return "ok";
        }).await().atMost(Duration.ofSeconds(5));

        assertFalse(executingThread.get().isVirtual(),
            "supply must not select virtual threads based on owner object");
    }

    // ---- emitList ----

    @Test
    void emitListRunsOnWorkerThreadNotVirtualThread() {
        AtomicReference<Thread> executingThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();

        List<String> result = BlockingExecutions.<String>emitList(this, () -> {
            executingThread.set(Thread.currentThread());
            return List.of("a", "b");
        }).collect().asList().await().atMost(Duration.ofSeconds(5));

        assertNotEquals(caller, executingThread.get(), "emitList must not run on the caller thread");
        assertFalse(executingThread.get().isVirtual(), "emitList must use a worker (platform) thread, not a virtual thread");
        assertEquals(List.of("a", "b"), result);
    }

    @Test
    void emitListWithNullOwnerRunsOnWorkerThread() {
        AtomicReference<Thread> executingThread = new AtomicReference<>();

        List<String> result = BlockingExecutions.<String>emitList(null, () -> {
            executingThread.set(Thread.currentThread());
            return List.of("x");
        }).collect().asList().await().atMost(Duration.ofSeconds(5));

        assertFalse(executingThread.get().isVirtual(), "emitList with null owner must use a worker thread");
        assertEquals(List.of("x"), result);
    }

    // ---- emitIterator ----

    @Test
    void emitIteratorRunsOnWorkerThreadNotVirtualThread() {
        AtomicReference<Thread> openThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();

        List<String> result = BlockingExecutions.<String>emitIterator(this, () -> {
            openThread.set(Thread.currentThread());
            return new CloseableIterator<>() {
                private int index;

                @Override
                public boolean hasNext() {
                    return index < 2;
                }

                @Override
                public String next() {
                    return "item-" + index++;
                }

                @Override
                public void close() {
                }
            };
        }).collect().asList().await().atMost(Duration.ofSeconds(5));

        assertNotEquals(caller, openThread.get(), "emitIterator must not open the iterator on the caller thread");
        assertFalse(openThread.get().isVirtual(), "emitIterator must use a worker (platform) thread, not a virtual thread");
        assertEquals(List.of("item-0", "item-1"), result);
    }

    @Test
    void emitIteratorWithNullOwnerRunsOnWorkerThread() {
        AtomicReference<Thread> openThread = new AtomicReference<>();

        List<String> result = BlockingExecutions.<String>emitIterator(null, () -> {
            openThread.set(Thread.currentThread());
            return new CloseableIterator<>() {
                private boolean done;

                @Override
                public boolean hasNext() {
                    return !done;
                }

                @Override
                public String next() {
                    done = true;
                    return "only";
                }

                @Override
                public void close() {
                }
            };
        }).collect().asList().await().atMost(Duration.ofSeconds(5));

        assertFalse(openThread.get().isVirtual(), "emitIterator with null owner must use a worker thread");
        assertEquals(List.of("only"), result);
    }
}
