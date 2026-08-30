package org.pipelineframework.blocking;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
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

        AssertSubscriber<String> subscriber = support.emitIterator(false, () -> new CloseableIterator<String>() {
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
        }).subscribe().withSubscriber(AssertSubscriber.create(1));

        subscriber.awaitItems(1);
        subscriber.cancel();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
    }

    @Test
    void emitIteratorReadsOnlyWhenDownstreamRequestsItems() {
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger hasNextCalls = new AtomicInteger();

        AssertSubscriber<String> subscriber = support.emitIterator(false, () -> new CloseableIterator<String>() {
            private int index;

            @Override
            public boolean hasNext() {
                hasNextCalls.incrementAndGet();
                return index < 3;
            }

            @Override
            public String next() {
                nextCalls.incrementAndGet();
                return "item-" + ++index;
            }

            @Override
            public void close() {
            }
        }).subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.assertHasNotReceivedAnyItem();
        assertTrue(nextCalls.get() == 0);
        assertTrue(hasNextCalls.get() == 0);

        subscriber.request(1).awaitItems(1);
        assertTrue(nextCalls.get() == 1);
        subscriber.assertItems("item-1");

        subscriber.request(1).awaitItems(2);
        assertTrue(nextCalls.get() == 2);
        subscriber.assertItems("item-1", "item-2");

        subscriber.request(1).awaitItems(3);
        assertTrue(nextCalls.get() == 3);
        subscriber.assertItems("item-1", "item-2", "item-3");
        subscriber.awaitCompletion(Duration.ofSeconds(5));
        subscriber.assertCompleted();
    }

    @Test
    void openIteratorTerminationWaitsForResourceClosure() throws Exception {
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        BlockingIteratorPublisher<String> opened = support.openIterator(false, () -> new CloseableIterator<>() {
            private boolean emitted;

            @Override
            public boolean hasNext() {
                return !emitted;
            }

            @Override
            public String next() {
                emitted = true;
                return "row";
            }

            @Override
            public void close() throws Exception {
                closeStarted.countDown();
                assertTrue(allowClose.await(5, TimeUnit.SECONDS));
            }
        });
        AssertSubscriber<String> subscriber = Multi.createFrom().publisher(opened.rows())
            .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitItems(1);
        assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
        assertFalse(opened.termination().toCompletableFuture().isDone());
        allowClose.countDown();

        subscriber.awaitCompletion(Duration.ofSeconds(5)).assertCompleted();
        opened.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void openIteratorCancellationDuringAcquisitionClosesBeforeTermination() throws Exception {
        CountDownLatch acquisitionStarted = new CountDownLatch(1);
        CountDownLatch allowAcquisition = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        BlockingIteratorPublisher<String> opened = support.openIterator(false, () -> {
            acquisitionStarted.countDown();
            try {
                assertTrue(allowAcquisition.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(failure);
            }
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public String next() {
                    return "unexpected";
                }

                @Override
                public void close() {
                    closed.countDown();
                }
            };
        });
        CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        opened.rows().subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription.complete(value);
                value.request(1);
            }

            @Override
            public void onNext(String item) {
                throw new AssertionError("cancelled acquisition emitted " + item);
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertTrue(acquisitionStarted.await(5, TimeUnit.SECONDS));
        subscription.get(5, TimeUnit.SECONDS).cancel();
        allowAcquisition.countDown();

        assertTrue(closed.await(5, TimeUnit.SECONDS));
        opened.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void openIteratorCancellationBeforeDemandSkipsAcquisition() throws Exception {
        AtomicBoolean acquired = new AtomicBoolean();
        BlockingIteratorPublisher<String> opened = support.openIterator(false, () -> {
            acquired.set(true);
            throw new AssertionError("cancelled iterator must not be acquired");
        });
        CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        opened.rows().subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription.complete(value);
            }

            @Override
            public void onNext(String item) {
                throw new AssertionError("cancelled iterator emitted " + item);
            }

            @Override
            public void onError(Throwable failure) {
            }

            @Override
            public void onComplete() {
            }
        });

        subscription.get(5, TimeUnit.SECONDS).cancel();
        opened.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertFalse(acquired.get());
    }

    @Test
    void openIteratorClosesAnEmptyResourceBeforeCompletingTermination() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        BlockingIteratorPublisher<String> opened = support.openIterator(false, () -> new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public String next() {
                throw new AssertionError("empty iterator has no row");
            }

            @Override
            public void close() {
                closed.set(true);
            }
        });
        AssertSubscriber<String> subscriber = Multi.createFrom().publisher(opened.rows())
            .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitCompletion(Duration.ofSeconds(5)).assertCompleted().assertHasNotReceivedAnyItem();
        opened.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(closed.get());
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
    void blockingIteratorServiceReactiveAdapterRunsOnWorkerThreadByDefault() {
        AtomicReference<Thread> openThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();

        List<String> values = new DefaultIteratorService(openThread).process("ok")
            .collect()
            .asList()
            .await()
            .atMost(Duration.ofSeconds(5));

        assertNotEquals(caller, openThread.get());
        assertFalse(openThread.get().isVirtual());
        assertTrue(values.equals(List.of("ok-1", "ok-2")));
    }

    static final class DefaultIteratorService implements BlockingIteratorService<String, String> {
        private final AtomicReference<Thread> openThread;

        private DefaultIteratorService(AtomicReference<Thread> openThread) {
            this.openThread = openThread;
        }

        @Override
        public CloseableIterator<String> iterateBlocking(String processableObj) {
            openThread.set(Thread.currentThread());
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
