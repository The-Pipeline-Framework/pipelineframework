package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

class PipelineInvocationRuntimeTest {
    private final PipelineInvocationRuntime runtime = new PipelineInvocationRuntime();
    private final TestBoundary boundary = new TestBoundary();

    @Test
    void transportUniReturnsOriginalItem() {
        String result = runtime.invokeTransportUni(boundary, () -> Uni.createFrom().item("ok"))
            .await().indefinitely();

        assertEquals("ok", result);
    }

    @Test
    void transportUniRethrowsFailureUnchanged() {
        IllegalArgumentException failure = new IllegalArgumentException("boom");

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> runtime.invokeTransportUni(boundary, () -> Uni.createFrom().failure(failure)).await().indefinitely());

        assertSame(failure, thrown);
    }

    @Test
    void transportUniRethrowsSupplierFailureUnchanged() {
        IllegalStateException failure = new IllegalStateException("boom");

        IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> runtime.invokeTransportUni(boundary, () -> {
                throw failure;
            }).await().indefinitely());

        assertSame(failure, thrown);
    }

    @Test
    void transportUniRejectsNullUniWithBoundarySpecificMessage() {
        IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> runtime.<String>invokeTransportUni(boundary, () -> null).await().indefinitely());

        assertEquals("Transport boundary invocation returned null Uni", thrown.getMessage());
    }

    @Test
    void transportMultiReturnsOriginalStream() {
        List<String> result = runtime.invokeTransportMulti(boundary, () -> Multi.createFrom().items("a", "b"))
            .collect().asList()
            .await().indefinitely();

        assertEquals(List.of("a", "b"), result);
    }

    @Test
    void transportMultiRethrowsFailureUnchanged() {
        IllegalArgumentException failure = new IllegalArgumentException("boom");

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> runtime.invokeTransportMulti(boundary, () -> Multi.createFrom().failure(failure))
                .collect().asList()
                .await().indefinitely());

        assertSame(failure, thrown);
    }

    @Test
    void transportMultiRejectsNullMultiWithBoundarySpecificMessage() {
        IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> runtime.<String>invokeTransportMulti(boundary, () -> null)
                .collect().asList()
                .await().indefinitely());

        assertEquals("Transport boundary invocation returned null Multi", thrown.getMessage());
    }

    @Test
    void transportMultiCancellationDoesNotChangeStreamBehavior() {
        AtomicBoolean cancelled = new AtomicBoolean();
        Multi<String> stream = Multi.createFrom().emitter(emitter -> {
            emitter.emit("first");
            emitter.onTermination(() -> cancelled.set(true));
        });

        AssertSubscriber<String> subscriber = runtime.invokeTransportMulti(boundary, () -> stream)
            .subscribe().withSubscriber(AssertSubscriber.create(1));

        subscriber.awaitItems(1, Duration.ofSeconds(2));
        subscriber.assertItems("first");
        subscriber.cancel();

        assertTrue(cancelled.get());
    }

    @Test
    void stepAndWorkerNullDiagnosticsRemainBehaviorSpecific() {
        IllegalStateException stepFailure = assertThrows(
            IllegalStateException.class,
            () -> runtime.<String>invokeStepUni(null, null, () -> null).await().indefinitely());
        IllegalStateException workerFailure = assertThrows(
            IllegalStateException.class,
            () -> runtime.<String>invokeTransitionWorker(null, () -> null).await().indefinitely());

        assertEquals("Step invocation returned null Uni", stepFailure.getMessage());
        assertEquals("Transition worker invocation returned null Uni", workerFailure.getMessage());
    }

    @Test
    void transportBoundaryRequiresDescriptor() {
        NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> runtime.invokeTransportUni(() -> null, () -> Uni.createFrom().item("ignored")));

        assertEquals("transport boundary must not be null", thrown.getMessage());
    }

    @Test
    void transportBoundaryRequiresBoundary() {
        NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> runtime.invokeTransportUni(null, () -> Uni.createFrom().item("ignored")));

        assertEquals("boundary must not be null", thrown.getMessage());
    }

    @Test
    void transportBoundaryPreservesAmbientContextAtCreationTime() {
        PipelineContext boundaryPipeline = new PipelineContext("boundary", "tenant-boundary", "prefer-cache");
        AwaitExecutionContext boundaryAwait = new AwaitExecutionContext("tenant-boundary", "exec-boundary", 5);
        AtomicReference<PipelineContext> observedPipeline = new AtomicReference<>();
        AtomicReference<AwaitExecutionContext> observedAwait = new AtomicReference<>();

        PipelineContextHolder.set(boundaryPipeline);
        AwaitExecutionContextHolder.set(boundaryAwait);
        Uni<String> result;
        try {
            result = runtime.invokeTransportUni(boundary, () -> {
                observedPipeline.set(PipelineContextHolder.get());
                observedAwait.set(AwaitExecutionContextHolder.get());
                return Uni.createFrom().item("ok");
            });
        } finally {
            PipelineContextHolder.clear();
            AwaitExecutionContextHolder.clear();
        }

        assertEquals("ok", result.await().indefinitely());
        assertSame(boundaryPipeline, observedPipeline.get());
        assertSame(boundaryAwait, observedAwait.get());
        assertNull(PipelineContextHolder.get());
        assertNull(AwaitExecutionContextHolder.get());
    }

    @Test
    void transportMultiSupplierFailureKeepsType() {
        IllegalStateException failure = new IllegalStateException("boom");

        Throwable thrown = assertThrows(
            IllegalStateException.class,
            () -> runtime.invokeTransportMulti(boundary, () -> {
                throw failure;
            }).collect().asList().await().indefinitely());

        assertSame(failure, thrown);
        assertInstanceOf(IllegalStateException.class, thrown);
    }

    @Test
    void stepUniRestoresInstallerThreadContextBeforeAsyncTermination() throws Exception {
        PipelineContext previousPipeline = new PipelineContext("previous", "previous-tenant", "previous-cache");
        AwaitExecutionContext previousAwait = new AwaitExecutionContext("previous-tenant", "previous-exec", 1);
        PipelineContext currentPipeline = new PipelineContext("current", "tenant-async", "prefer-cache");
        AwaitExecutionContext currentAwait = new AwaitExecutionContext("tenant-async", "exec-async", 2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<String> observed = new AtomicReference<>();

        PipelineContextHolder.set(previousPipeline);
        AwaitExecutionContextHolder.set(previousAwait);
        try {
            Uni<String> result = runtime.invokeStepUni(currentPipeline, currentAwait, () ->
                Uni.createFrom().emitter(emitter -> new Thread(() -> {
                    await(release);
                    emitter.complete("done");
                }, "invocation-runtime-uni-test").start()));

            result.onItem().invoke(item -> observed.set(
                    PipelineContextHolder.get().replayMode() + ":" + AwaitExecutionContextHolder.get().executionId()))
                .subscribe().with(item -> {
                });

            assertSame(previousPipeline, PipelineContextHolder.get());
            assertSame(previousAwait, AwaitExecutionContextHolder.get());

            release.countDown();
            awaitUntilSet(observed);
            assertEquals("tenant-async:exec-async", observed.get());
            assertSame(previousPipeline, PipelineContextHolder.get());
            assertSame(previousAwait, AwaitExecutionContextHolder.get());
        } finally {
            PipelineContextHolder.clear();
            AwaitExecutionContextHolder.clear();
        }
    }

    @Test
    void stepMultiRestoresSignalThreadContextAfterAsyncSignal() throws Exception {
        PipelineContext currentPipeline = new PipelineContext("current", "tenant-signal", "prefer-cache");
        AwaitExecutionContext currentAwait = new AwaitExecutionContext("tenant-signal", "exec-signal", 3);
        PipelineContext signalPrevious = new PipelineContext("signal-previous", "signal-tenant", "signal-cache");
        AwaitExecutionContext signalAwaitPrevious = new AwaitExecutionContext("signal-tenant", "signal-exec", 4);
        CountDownLatch signalDone = new CountDownLatch(1);
        AtomicReference<String> observedDuringSignal = new AtomicReference<>();
        AtomicReference<PipelineContext> signalThreadAfterPipeline = new AtomicReference<>();
        AtomicReference<AwaitExecutionContext> signalThreadAfterAwait = new AtomicReference<>();

        Multi<String> result = runtime.invokeStepMulti(currentPipeline, currentAwait, () ->
            Multi.createFrom().emitter(emitter -> new Thread(() -> {
                PipelineContextHolder.set(signalPrevious);
                AwaitExecutionContextHolder.set(signalAwaitPrevious);
                try {
                    emitter.emit("value");
                    emitter.complete();
                    signalThreadAfterPipeline.set(PipelineContextHolder.get());
                    signalThreadAfterAwait.set(AwaitExecutionContextHolder.get());
                } finally {
                    PipelineContextHolder.clear();
                    AwaitExecutionContextHolder.clear();
                    signalDone.countDown();
                }
            }, "invocation-runtime-multi-test").start()));

        List<String> values = result
            .onItem().invoke(item -> observedDuringSignal.set(
                PipelineContextHolder.get().replayMode() + ":" + AwaitExecutionContextHolder.get().executionId()))
            .collect().asList()
            .await().atMost(Duration.ofSeconds(5));

        assertEquals(List.of("value"), values);
        assertTrue(signalDone.await(5, TimeUnit.SECONDS));
        assertEquals("tenant-signal:exec-signal", observedDuringSignal.get());
        assertSame(signalPrevious, signalThreadAfterPipeline.get());
        assertSame(signalAwaitPrevious, signalThreadAfterAwait.get());
    }

    private final class TestBoundary implements TransportBoundaryInvocation {
        @Override
        public TransportBoundaryDescriptor transportBoundary() {
            return new TransportBoundaryDescriptor("test", "target");
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void awaitUntilSet(AtomicReference<?> reference) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (reference.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(reference.get() != null, "value was not observed before timeout");
    }
}
