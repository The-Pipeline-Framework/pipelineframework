package org.pipelineframework.blocking;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.subscription.Cancellable;
import org.junit.jupiter.api.Test;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.blocking.BlockingService;
import org.pipelineframework.service.blocking.BlockingIteratorService;
import org.pipelineframework.service.blocking.BlockingStreamingService;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingExecutionSupportTest {

    private final BlockingExecutionSupport support = new BlockingExecutionSupport();

    @Test
    void workerExecutionRunsOffCallerThread() {
        AtomicReference<Thread> executingThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();

        String value = support.supply(false, () -> {
            executingThread.set(Thread.currentThread());
            return "ok";
        }).await().atMost(Duration.ofSeconds(5));

        assertNotEquals(caller, executingThread.get());
        assertFalse(executingThread.get().isVirtual());
        assertTrue("ok".equals(value));
    }

    @Test
    void virtualThreadExecutionUsesVirtualThreads() {
        AtomicBoolean isVirtual = new AtomicBoolean(false);

        String value = support.supply(true, () -> {
            isVirtual.set(Thread.currentThread().isVirtual());
            return "ok";
        }).await().atMost(Duration.ofSeconds(5));

        assertTrue(isVirtual.get());
        assertTrue("ok".equals(value));
    }

    @Test
    void blockingUnaryServiceReactiveAdapterRunsOffCallerThread() {
        AtomicReference<Thread> executingThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();

        BlockingService<String, String> service = new BlockingService<>() {
            @Override
            public String processBlocking(String processableObj) {
                executingThread.set(Thread.currentThread());
                return processableObj + "-done";
            }
        };

        String value = service.process("ok").await().atMost(Duration.ofSeconds(5));

        assertNotEquals(caller, executingThread.get());
        assertFalse(executingThread.get().isVirtual());
        assertTrue("ok-done".equals(value));
    }

    @Test
    void blockingStreamingServiceReactiveAdapterRunsOffCallerThread() {
        AtomicReference<Thread> executingThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();

        BlockingStreamingService<String, String> service = new BlockingStreamingService<>() {
            @Override
            public List<String> processBlocking(String processableObj) {
                executingThread.set(Thread.currentThread());
                return List.of(processableObj + "-1", processableObj + "-2");
            }
        };

        List<String> values = service.process("ok")
            .collect()
            .asList()
            .await()
            .atMost(Duration.ofSeconds(5));

        assertNotEquals(caller, executingThread.get());
        assertFalse(executingThread.get().isVirtual());
        assertTrue(values.equals(List.of("ok-1", "ok-2")));
    }

    @Test
    void emitIteratorRunsAcquisitionAndIterationOffCallerThreadAndClosesOnCompletion() throws Exception {
        AtomicReference<Thread> openThread = new AtomicReference<>();
        AtomicReference<Thread> iterationThread = new AtomicReference<>();
        CountDownLatch closed = new CountDownLatch(1);
        Thread caller = Thread.currentThread();

        io.smallrye.mutiny.Multi<String> emitted = support.emitIterator(false, () -> {
            openThread.set(Thread.currentThread());
            return new CloseableIterator<String>() {
                private int index;

                @Override
                public boolean hasNext() {
                    iterationThread.compareAndSet(null, Thread.currentThread());
                    return index < 2;
                }

                @Override
                public String next() {
                    iterationThread.compareAndSet(null, Thread.currentThread());
                    return index++ == 0 ? "a" : "b";
                }

                @Override
                public void close() {
                    closed.countDown();
                }
            };
        });
        List<String> values = emitted.collect().asList().await().atMost(Duration.ofSeconds(5));

        assertTrue(values.equals(List.of("a", "b")));
        assertNotEquals(caller, openThread.get());
        assertNotEquals(caller, iterationThread.get());
        assertFalse(openThread.get().isVirtual());
        assertFalse(iterationThread.get().isVirtual());
        assertTrue(closed.await(5, TimeUnit.SECONDS));
    }

    @Test
    void emitIteratorClosesOnCancellation() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        AtomicReference<Cancellable> cancellable = new AtomicReference<>();

        Cancellable subscription = support.emitIterator(false, () -> new CloseableIterator<String>() {
            private int index;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public String next() {
                if (index++ == 0) {
                    return "first";
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "later";
            }

            @Override
            public void close() {
                closed.countDown();
            }
        }).subscribe().with(item -> {
            if ("first".equals(item)) {
                Cancellable current = cancellable.get();
                if (current != null) {
                    current.cancel();
                }
            }
        });
        cancellable.set(subscription);

        assertTrue(closed.await(5, TimeUnit.SECONDS));
    }

    @Test
    void emitIteratorClosesOnFailure() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);

        RuntimeException failure = assertThrows(RuntimeException.class, () -> support.emitIterator(false, () -> new CloseableIterator<String>() {
            private boolean emitted;

            @Override
            public boolean hasNext() {
                if (emitted) {
                    throw new RuntimeException("boom");
                }
                return true;
            }

            @Override
            public String next() {
                emitted = true;
                return "first";
            }

            @Override
            public void close() {
                closed.countDown();
            }
        }).collect().asList().await().atMost(Duration.ofSeconds(5)));

        assertTrue(failure.getMessage().contains("boom"));
        assertTrue(closed.await(5, TimeUnit.SECONDS));
    }

    @Test
    void blockingIteratorServiceReactiveAdapterUsesVirtualThreadsWhenAnnotated() {
        AtomicBoolean openOnVirtualThread = new AtomicBoolean(false);

        List<String> values = new VirtualIteratorService(openOnVirtualThread).process("ok")
            .collect()
            .asList()
            .await()
            .atMost(Duration.ofSeconds(5));

        assertTrue(openOnVirtualThread.get());
        assertTrue(values.equals(List.of("ok-1", "ok-2")));
    }

    @PipelineStep(runOnVirtualThreads = true)
    static final class VirtualIteratorService implements BlockingIteratorService<String, String> {
        private final AtomicBoolean openOnVirtualThread;

        private VirtualIteratorService(AtomicBoolean openOnVirtualThread) {
            this.openOnVirtualThread = openOnVirtualThread;
        }

        @Override
        public CloseableIterator<String> iterateBlocking(String processableObj) {
            openOnVirtualThread.set(Thread.currentThread().isVirtual());
            return new CloseableIterator<String>() {
                private int index;

                @Override
                public boolean hasNext() {
                    return index < 2;
                }

                @Override
                public String next() {
                    return processableObj + "-" + ++index;
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
