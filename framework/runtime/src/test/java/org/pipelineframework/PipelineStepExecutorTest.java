/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.quarkus.arc.ClientProxy;
import io.quarkus.arc.InjectableBean;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.AwaitStreamOneToOneStep;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.branching.PipelineBranchRoutingException;
import org.pipelineframework.branching.StepBranchingDescriptor;
import org.pipelineframework.blocking.CloseableIterator;
import org.pipelineframework.blocking.BlockingExecutionSupport;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.blocking.StepManyToManyBlocking;
import org.pipelineframework.step.blocking.StepManyToOneBlocking;
import org.pipelineframework.step.blocking.StepOneToManyBlocking;
import org.pipelineframework.step.blocking.StepOneToManyBlockingIterator;
import org.pipelineframework.step.blocking.StepOneToOneBlocking;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;

import static java.util.Optional.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineStepExecutorTest {
    @AfterEach
    void clearCapturedContexts() {
        PipelineContextHolder.clear();
        AwaitExecutionContextHolder.clear();
    }

    @Test
    void oneToOneOnUniExecutesAndPropagatesContext() {
        PipelineContext context = new PipelineContext("v1", "tenant", "prefer-cache");

        String value = PipelineStepExecutor.withPipelineContext(
            context,
            () -> PipelineContextHolder.get().replayMode() + ":payload");

        assertEquals("tenant:payload", value);
    }

    @Test
    void oneToOneOnMultiSequentialProducesAllItems() {
        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            new SuffixOneToOneStep("-done"),
            Multi.createFrom().items("a", "b"),
            false,
            16,
            null,
            null,
            null,
            null);

        assertEquals(List.of("a-done", "b-done"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void applyStepUnwrapsArcClientProxyBeforeDispatch() {
        PipelineStepExecutor executor = new PipelineStepExecutor();

        Object result = executor.applyStep(
            new ArcProxyOnlyStep(new SuffixOneToOneStep("-done")),
            Uni.createFrom().item("a"),
            org.pipelineframework.config.ParallelismPolicy.AUTO,
            16,
            null,
            null,
            null,
            null,
            null);

        assertEquals("a-done", ((Uni<String>) result).await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void applyStepExecutesReactiveServiceContract() {
        PipelineStepExecutor executor = new PipelineStepExecutor();

        Object result = executor.applyStep(
            (ReactiveService<String, String>) input -> Uni.createFrom().item(input + "-service"),
            Uni.createFrom().item("a"),
            org.pipelineframework.config.ParallelismPolicy.AUTO,
            16,
            null,
            null,
            null,
            null,
            null);

        assertEquals("a-service", ((Uni<String>) result).await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void applyStepExecutesReactiveStreamingServiceContract() {
        PipelineStepExecutor executor = new PipelineStepExecutor();

        Object result = executor.applyStep(
            (ReactiveStreamingService<String, String>) input -> Multi.createFrom().items(input + "-1", input + "-2"),
            Uni.createFrom().item("a"),
            org.pipelineframework.config.ParallelismPolicy.AUTO,
            16,
            null,
            null,
            null,
            null,
            null);

        assertEquals(List.of("a-1", "a-2"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void applyStepExecutesReactiveBidirectionalStreamingServiceContract() {
        PipelineStepExecutor executor = new PipelineStepExecutor();

        Object result = executor.applyStep(
            (ReactiveBidirectionalStreamingService<String, String>) input -> input.map(item -> item + "-mapped"),
            Multi.createFrom().items("a", "b"),
            org.pipelineframework.config.ParallelismPolicy.AUTO,
            16,
            null,
            null,
            null,
            null,
            null);

        assertEquals(List.of("a-mapped", "b-mapped"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void oneToOneStepInvocationInstallsAndRestoresPipelineAndAwaitContext() {
        PipelineContext previousPipeline = new PipelineContext("previous", "previous-tenant", "previous-cache");
        AwaitExecutionContext previousAwait = new AwaitExecutionContext("previous-tenant", "previous-exec", 1);
        PipelineContext currentPipeline = new PipelineContext("current", "tenant-1", "prefer-cache");
        AwaitExecutionContext currentAwait = new AwaitExecutionContext("tenant-1", "exec-1", 2);
        PipelineContextHolder.set(previousPipeline);
        AwaitExecutionContextHolder.set(previousAwait);

        try {
            Object result = PipelineStepExecutor.applyOneToOneUnchecked(
                new ContextCapturingOneToOneStep(),
                Uni.createFrom().item("payload"),
                false,
                16,
                null,
                null,
                null,
                currentPipeline,
                currentAwait);

            assertEquals("tenant-1:exec-1:payload", ((Uni<String>) result).await().atMost(Duration.ofSeconds(5)));
            assertSame(previousPipeline, PipelineContextHolder.get());
            assertSame(previousAwait, AwaitExecutionContextHolder.get());
        } finally {
            PipelineContextHolder.clear();
            AwaitExecutionContextHolder.clear();
        }
    }

    @Test
    void oneToOneFutureOnMultiParallelProducesAllItems() {
        Object result = PipelineStepExecutor.applyOneToOneFutureUnchecked(
            new FutureSuffixStep("-future"),
            Multi.createFrom().items("a", "b"),
            true,
            16,
            null,
            null,
            null);

        List<String> values = ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5));
        values.sort(String::compareTo);
        assertEquals(List.of("a-future", "b-future"), values);
    }

    @Test
    void branchAwareOneToOneSkipsNonApplicableItemsAndPassesThemThrough() {
        ReserveStockStep step = new ReserveStockStep();
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            1,
            "Reserve Stock",
            step.getClass().getName(),
            PhysicalOrder.class.getName(),
            PhysicalOrder.class,
            List.of("PhysicalOrder"),
            List.of(PhysicalOrder.class.getName()),
            List.of(PhysicalOrder.class),
            false);

        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item(new DigitalOrder("o-1")),
            false,
            16,
            null,
            null,
            null,
            null,
            null,
            descriptor);

        Object output = ((Uni<?>) result).await().atMost(Duration.ofSeconds(5));
        assertTrue(output instanceof DigitalOrder);
        assertEquals("o-1", ((DigitalOrder) output).id());
        assertEquals(0, step.invocations());
    }

    @Test
    void branchAwareOneToOneExtractsAcceptedUnionVariantBeforeInvokingStep() {
        ApprovePaymentStep step = new ApprovePaymentStep();
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            2,
            "Approve Payment",
            step.getClass().getName(),
            ApprovedPaymentStatusMessage.class.getName(),
            ApprovedPaymentStatusMessage.class,
            List.of("ApprovedPaymentStatus"),
            List.of(ApprovedPaymentStatusMessage.class.getName()),
            List.of(ApprovedPaymentStatusMessage.class),
            false);

        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item(PaymentStatusEnvelope.approved("p-1")),
            false,
            16,
            null,
            null,
            null,
            null,
            null,
            descriptor);

        Object output = ((Uni<?>) result).await().atMost(Duration.ofSeconds(5));
        assertEquals("approved:p-1", output);
        assertEquals(1, step.invocations());
    }

    @Test
    void branchAwareTerminalRejectsUnexpectedRuntimeType() {
        ReserveStockStep step = new ReserveStockStep();
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            4,
            "Finalize",
            step.getClass().getName(),
            StockReserved.class.getName(),
            StockReserved.class,
            List.of("StockReserved"),
            List.of(StockReserved.class.getName()),
            List.of(StockReserved.class),
            true);

        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item(new DigitalOrder("o-2")),
            false,
            16,
            null,
            null,
            null,
            null,
            null,
            descriptor);

        PipelineBranchRoutingException exception = assertThrows(
            PipelineBranchRoutingException.class,
            () -> ((Uni<?>) result).await().atMost(Duration.ofSeconds(5)));
        assertTrue(exception.getMessage().contains("Finalize"));
        assertTrue(exception.getMessage().contains(DigitalOrder.class.getName()));
    }

    @Test
    void branchAwareOneToOneWrapsAcceptedVariantIntoTerminalUnionInput() {
        FinalizePaymentStep step = new FinalizePaymentStep();
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            5,
            "Finalize Payment Output",
            step.getClass().getName(),
            PaymentOutputBranchEnvelope.class.getName(),
            PaymentOutputBranchEnvelope.class,
            List.of("ApprovedPaymentOutput"),
            List.of(ApprovedPaymentOutputMessage.class.getName()),
            List.of(ApprovedPaymentOutputMessage.class),
            true);

        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item(new ApprovedPaymentOutputMessage("p-42")),
            false,
            16,
            null,
            null,
            null,
            null,
            null,
            descriptor);

        Object output = ((Uni<?>) result).await().atMost(Duration.ofSeconds(5));
        assertEquals("finalized:p-42", output);
        assertEquals(1, step.invocations());
    }

    @Test
    void branchAwareOneToOneRewrapsExtractedUnionVariantIntoTerminalInput() {
        FinalizePaymentStep step = new FinalizePaymentStep();
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            6,
            "Finalize Payment Output",
            step.getClass().getName(),
            PaymentOutputBranchEnvelope.class.getName(),
            PaymentOutputBranchEnvelope.class,
            List.of("ApprovedPaymentOutput"),
            List.of(ApprovedPaymentOutputMessage.class.getName()),
            List.of(ApprovedPaymentOutputMessage.class),
            true);

        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item(PaymentOutputBranchEnvelope.newBuilder()
                .setApproved(new ApprovedPaymentOutputMessage("p-43"))
                .build()),
            false,
            16,
            null,
            null,
            null,
            null,
            null,
            descriptor);

        Object output = ((Uni<?>) result).await().atMost(Duration.ofSeconds(5));
        assertEquals("finalized:p-43", output);
        assertEquals(1, step.invocations());
    }

    @Test
    void oneToManyMergeProducesExpandedItems() {
        Object result = PipelineStepExecutor.applyOneToManyUnchecked(
            new ExpandingOneToManyStep(),
            Uni.createFrom().item("x"),
            true,
            16,
            null,
            null,
            null);

        assertEquals(List.of("x-1", "x-2"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void oneToManyAwaitParentUsesDemandWithoutSyntheticOverflowBuffer() {
        AtomicInteger emitted = new AtomicInteger();
        class DemandAwareStep extends ConfigurableStep implements StepOneToMany<String, String> {
            @Override
            public Multi<String> applyOneToMany(String input) {
                return Multi.createFrom().emitter(emitter -> emitter.onRequest(requested -> {
                    for (long i = 0; i < requested && emitted.get() < 10; i++) {
                        int next = emitted.incrementAndGet();
                        emitter.emit(input + "-" + next);
                    }
                    if (emitted.get() == 10) {
                        emitter.complete();
                    }
                }), BackPressureStrategy.ERROR);
            }
        }
        StepOneToMany<String, String> step = new DemandAwareStep();

        Object result = PipelineStepExecutor.applyOneToManyUnchecked(
            step,
            Uni.createFrom().item("x"),
            false,
            16,
            null,
            null,
            null,
            new AwaitExecutionContext("tenant", "execution", 0));

        AssertSubscriber<String> subscriber = ((Multi<String>) result)
            .subscribe()
            .withSubscriber(AssertSubscriber.create(0));

        subscriber.assertHasNotReceivedAnyItem();
        assertEquals(0, emitted.get());

        subscriber.request(1).awaitItems(1, Duration.ofSeconds(5));

        subscriber.assertItems("x-1");
        assertEquals(1, emitted.get());
    }

    @Test
    void oneToManyAwaitParentAppliesRetryRecoveryAndRejectPoliciesWithoutSyntheticBuffer() {
        RecoveringOneToManyStep step = new RecoveringOneToManyStep();

        try {
            Object result = PipelineStepExecutor.applyOneToManyUnchecked(
                step,
                Uni.createFrom().item("bad"),
                false,
                16,
                null,
                null,
                null,
                new AwaitExecutionContext("tenant", "execution", 0));

            AssertSubscriber<String> subscriber = ((Multi<String>) result)
                .subscribe()
                .withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
            subscriber.awaitCompletion(Duration.ofSeconds(5));

            subscriber.assertCompleted();
            subscriber.assertHasNotReceivedAnyItem();
            assertEquals(1, step.applyOneToManyCalls());
            assertTrue(step.rejectCalled());
        } finally {
            AwaitExecutionContextHolder.clear();
        }
    }

    @Test
    void oneToManyAwaitParentIteratorSourceIsRequestedBeforeAwaitSuspension() {
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger nextCalls = new AtomicInteger();
        BlockingExecutionSupport blocking = new BlockingExecutionSupport();
        class IteratorSourceStep extends ConfigurableStep implements StepOneToMany<String, String> {
            @Override
            public Multi<String> applyOneToMany(String input) {
                return blocking.emitIterator(false, () -> {
                    opened.incrementAndGet();
                    return new CloseableIterator<>() {
                        private int index;

                        @Override
                        public boolean hasNext() {
                            return index < 3;
                        }

                        @Override
                        public String next() {
                            index++;
                            nextCalls.incrementAndGet();
                            return input + "-" + index;
                        }

                        @Override
                        public void close() {
                            // no-op
                        }
                    };
                });
            }
        }
        class SuspendingAwaitStep implements AwaitStreamOneToOneStep<String, String> {
            @Override
            public Multi<String> applyAwaitPerItem(Multi<String> input) {
                return input.onItem()
                    .transformToUniAndConcatenate(item -> Uni.createFrom().item(item))
                    .collect()
                    .asList()
                    .onItem()
                    .transformToMulti(ignored -> Multi.createFrom().failure(
                        new AwaitSuspendedException("tenant", "execution", "unit", 1)));
            }
        }

        AwaitExecutionContext awaitContext = new AwaitExecutionContext("tenant", "execution", 0);
        Object source = PipelineStepExecutor.applyOneToManyUnchecked(
            new IteratorSourceStep(),
            Uni.createFrom().item("x"),
            false,
            16,
            null,
            null,
            null,
            awaitContext);
        Object awaited = new PipelineStepExecutor().applyStep(
            new SuspendingAwaitStep(),
            source,
            org.pipelineframework.config.ParallelismPolicy.AUTO,
            16,
            null,
            null,
            null,
            null,
            new AwaitExecutionContext("tenant", "execution", 1));

        AssertSubscriber<String> subscriber = ((Multi<String>) awaited)
            .subscribe()
            .withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        Throwable failure = subscriber.awaitFailure(Duration.ofSeconds(5)).getFailure();

        assertTrue(failure instanceof AwaitSuspendedException);
        assertEquals(1, opened.get());
        assertEquals(3, nextCalls.get());
    }

    @Test
    void awaitStreamStepIgnoresBranchingDescriptorAndPreservesWholeStreamSemantics() {
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger nextCalls = new AtomicInteger();
        BlockingExecutionSupport blocking = new BlockingExecutionSupport();
        class IteratorSourceStep extends ConfigurableStep implements StepOneToMany<String, String> {
            @Override
            public Multi<String> applyOneToMany(String input) {
                return blocking.emitIterator(false, () -> {
                    opened.incrementAndGet();
                    return new CloseableIterator<>() {
                        private int index;

                        @Override
                        public boolean hasNext() {
                            return index < 3;
                        }

                        @Override
                        public String next() {
                            index++;
                            nextCalls.incrementAndGet();
                            return input + "-" + index;
                        }

                        @Override
                        public void close() {
                        }
                    };
                });
            }
        }
        class SuspendingAwaitStep extends ConfigurableStep implements AwaitStreamOneToOneStep<String, String> {
            @Override
            public Multi<String> applyAwaitPerItem(Multi<String> input) {
                return input.onItem()
                    .transformToUniAndConcatenate(item -> Uni.createFrom().item(item))
                    .collect()
                    .asList()
                    .onItem()
                    .transformToMulti(ignored -> Multi.createFrom().failure(
                        new AwaitSuspendedException("tenant", "execution", "unit", 1)));
            }
        }

        AwaitExecutionContext awaitContext = new AwaitExecutionContext("tenant", "execution", 0);
        Object source = PipelineStepExecutor.applyOneToManyUnchecked(
            new IteratorSourceStep(),
            Uni.createFrom().item("x"),
            false,
            16,
            null,
            null,
            null,
            awaitContext);
        PipelineStepExecutor executor = new PipelineStepExecutor();
        executor.branchingRegistry = new org.pipelineframework.branching.PipelineBranchingRegistry() {
            @Override
            public java.util.Optional<StepBranchingDescriptor> descriptorFor(Class<?> stepClass) {
                return of(new StepBranchingDescriptor(
                    1,
                    "Await",
                    stepClass.getName(),
                    String.class.getName(),
                    String.class,
                    List.of("String"),
                    List.of(String.class.getName()),
                    List.of(String.class),
                    false));
            }
        };
        Object awaited = executor.applyStep(
            new SuspendingAwaitStep(),
            source,
            org.pipelineframework.config.ParallelismPolicy.AUTO,
            16,
            null,
            null,
            null,
            null,
            new AwaitExecutionContext("tenant", "execution", 1));

        AssertSubscriber<String> subscriber = ((Multi<String>) awaited)
            .subscribe()
            .withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        Throwable failure = subscriber.awaitFailure(Duration.ofSeconds(5)).getFailure();

        assertTrue(failure instanceof AwaitSuspendedException);
        assertEquals(1, opened.get());
        assertEquals(3, nextCalls.get());
    }

    @Test
    void oneToManyParallelRecoveryCompletesAfterRejectedInnerStream() {
        RecoveringOneToManyStep step = new RecoveringOneToManyStep();

        Object result = PipelineStepExecutor.applyOneToManyUnchecked(
            step,
            Multi.createFrom().items("first", "bad", "second"),
            true,
            16,
            null,
            null,
            null);

        List<String> values = ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5));

        assertEquals(Set.of("first-ok", "second-ok"), Set.copyOf(values));
        assertEquals(2, values.size());
        assertTrue(step.rejectCalled());
    }

    @Test
    void oneToManyParallelFailureStillFailsWhenRecoveryDisabled() {
        NonRecoveringOneToManyStep step = new NonRecoveringOneToManyStep();

        Object result = PipelineStepExecutor.applyOneToManyUnchecked(
            step,
            Multi.createFrom().items("first", "bad", "second"),
            true,
            16,
            null,
            null,
            null);

        io.smallrye.mutiny.helpers.test.AssertSubscriber<String> subscriber =
            ((Multi<String>) result).subscribe().withSubscriber(
                io.smallrye.mutiny.helpers.test.AssertSubscriber.create(2));
        subscriber.awaitFailure(Duration.ofSeconds(5));
        assertTrue(subscriber.getFailure() instanceof RuntimeException);
    }

    @Test
    void manyToOneFromMultiReducesItems() {
        Object result = PipelineStepExecutor.applyManyToOneUnchecked(
            (ManyToOne<String, String>) input -> input.collect().asList().map(items -> String.join(",", items)),
            Multi.createFrom().items("a", "b"),
            null,
            null,
            null);

        assertEquals("a,b", ((Uni<String>) result).await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void manyToManyFromUniProducesStream() {
        Object result = PipelineStepExecutor.applyManyToManyUnchecked(
            new MappingManyToManyStep(),
            Uni.createFrom().item("x"),
            null,
            null,
            null);

        assertEquals(List.of("x-mapped"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void awaitStreamOneToOneInvocationUsesSharedStepContextRuntime() {
        PipelineContext context = new PipelineContext("v1", "tenant-stream", "prefer-cache");
        AwaitExecutionContext awaitContext = new AwaitExecutionContext("tenant-stream", "exec-stream", 3);

        try {
            PipelineStepExecutor executor = new PipelineStepExecutor();
            Object result = executor.applyStep(
                new ContextCapturingAwaitStreamStep(),
                Multi.createFrom().items("a", "b"),
                org.pipelineframework.config.ParallelismPolicy.SEQUENTIAL,
                16,
                null,
                null,
                null,
                context,
                awaitContext);

            assertEquals(
                List.of("tenant-stream:exec-stream:a", "tenant-stream:exec-stream:b"),
                ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
            assertNull(PipelineContextHolder.get());
            assertNull(AwaitExecutionContextHolder.get());
        } finally {
            PipelineContextHolder.clear();
            AwaitExecutionContextHolder.clear();
        }
    }

    @Test
    void blockingOneToOneOnUniProducesOutput() {
        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            new BlockingSuffixOneToOneStep("-blocking"),
            Uni.createFrom().item("x"));

        assertEquals("x-blocking", ((Uni<String>) result).await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void blockingOneToManyProducesExpandedItems() {
        Object result = PipelineStepExecutor.applyOneToManyUnchecked(
            new BlockingExpandingOneToManyStep(),
            Uni.createFrom().item("x"),
            false,
            16,
            null,
            null,
            null);

        assertEquals(List.of("x-1", "x-2"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void blockingIteratorOneToManyProducesExpandedItems() {
        Object result = PipelineStepExecutor.applyOneToManyUnchecked(
            new BlockingIteratorOneToManyStep(),
            Uni.createFrom().item("x"),
            false,
            16,
            null,
            null,
            null);

        assertEquals(List.of("x-1", "x-2"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void blockingManyToOneReducesItems() {
        Object result = PipelineStepExecutor.applyManyToOneUnchecked(
            new BlockingReducingManyToOneStep(),
            Multi.createFrom().items("a", "b"),
            null,
            null,
            null);

        assertEquals("a,b", ((Uni<String>) result).await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void blockingManyToManyMapsItems() {
        Object result = PipelineStepExecutor.applyManyToManyUnchecked(
            new BlockingMappingManyToManyStep(),
            Multi.createFrom().items("a", "b"),
            null,
            null,
            null);

        assertEquals(List.of("a-mapped", "b-mapped"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    static final class SuffixOneToOneStep extends ConfigurableStep implements StepOneToOne<String, String> {
        private final String suffix;

        SuffixOneToOneStep(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom().item(input + suffix);
        }
    }

    static final class ArcProxyOnlyStep implements ClientProxy {
        private final Object delegate;

        ArcProxyOnlyStep(Object delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object arc_contextualInstance() {
            return delegate;
        }

        @Override
        public InjectableBean<?> arc_bean() {
            throw new UnsupportedOperationException("arc_bean is not used by this test proxy");
        }
    }

    static final class ContextCapturingOneToOneStep extends ConfigurableStep implements StepOneToOne<String, String> {
        @Override
        public Uni<String> applyOneToOne(String input) {
            PipelineContext pipelineContext = PipelineContextHolder.get();
            AwaitExecutionContext awaitContext = AwaitExecutionContextHolder.get();
            return Uni.createFrom().item(
                pipelineContext.replayMode() + ":" + awaitContext.executionId() + ":" + input);
        }
    }

    static final class ContextCapturingAwaitStreamStep implements AwaitStreamOneToOneStep<String, String> {
        @Override
        public Multi<String> applyAwaitPerItem(Multi<String> input) {
            return input.onItem().transform(item -> {
                PipelineContext pipelineContext = PipelineContextHolder.get();
                AwaitExecutionContext awaitContext = AwaitExecutionContextHolder.get();
                return pipelineContext.replayMode() + ":" + awaitContext.executionId() + ":" + item;
            });
        }
    }

    static final class FutureSuffixStep extends ConfigurableStep implements StepOneToOneCompletableFuture<String, String> {
        private final String suffix;

        FutureSuffixStep(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public CompletableFuture<String> applyAsync(String in) {
            return CompletableFuture.completedFuture(in + suffix);
        }
    }

    static final class ApprovePaymentStep extends ConfigurableStep
        implements StepOneToOne<ApprovedPaymentStatusMessage, String> {
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public Uni<String> applyOneToOne(ApprovedPaymentStatusMessage input) {
            invocations.incrementAndGet();
            return Uni.createFrom().item("approved:" + input.paymentId());
        }

        int invocations() {
            return invocations.get();
        }
    }

    static final class ExpandingOneToManyStep extends ConfigurableStep implements StepOneToMany<String, String> {
        @Override
        public Multi<String> applyOneToMany(String in) {
            return Multi.createFrom().items(in + "-1", in + "-2");
        }
    }

    static final class RecoveringOneToManyStep extends ConfigurableStep implements StepOneToMany<String, String> {
        private final AtomicBoolean rejectCalled = new AtomicBoolean(false);
        private final AtomicInteger applyOneToManyCalls = new AtomicInteger();

        @Override
        public Multi<String> applyOneToMany(String in) {
            applyOneToManyCalls.incrementAndGet();
            if ("bad".equals(in)) {
                return Multi.createFrom().failure(new RuntimeException("stream boom"));
            }
            return Multi.createFrom().emitter(emitter -> {
                new Thread(() -> {
                    try {
                        Thread.sleep(10);
                        emitter.emit(in + "-ok");
                        emitter.complete();
                    } catch (InterruptedException e) {
                        emitter.fail(e);
                    }
                }).start();
            });
        }

        @Override
        public org.pipelineframework.config.StepConfig effectiveConfig() {
            return new org.pipelineframework.config.StepConfig()
                .recoverOnFailure(true)
                .retryLimit(1)
                .retryWait(Duration.ofMillis(1));
        }

        @Override
        public Uni<String> rejectStream(
            List<String> sampleItems,
            long totalItemCount,
            Throwable cause,
            Integer retriesObserved,
            Integer retryLimit) {
            rejectCalled.set(true);
            return Uni.createFrom().nullItem();
        }

        boolean rejectCalled() {
            return rejectCalled.get();
        }

        int applyOneToManyCalls() {
            return applyOneToManyCalls.get();
        }
    }

    static final class NonRecoveringOneToManyStep extends ConfigurableStep implements StepOneToMany<String, String> {
        @Override
        public Multi<String> applyOneToMany(String in) {
            if ("bad".equals(in)) {
                return Multi.createFrom().failure(new RuntimeException("stream boom"));
            }
            return Multi.createFrom().emitter(emitter -> {
                new Thread(() -> {
                    try {
                        Thread.sleep(10);
                        emitter.emit(in + "-ok");
                        emitter.complete();
                    } catch (InterruptedException e) {
                        emitter.fail(e);
                    }
                }).start();
            });
        }

        @Override
        public org.pipelineframework.config.StepConfig effectiveConfig() {
            return new org.pipelineframework.config.StepConfig()
                .recoverOnFailure(false)
                .retryLimit(0)
                .retryWait(Duration.ofMillis(1));
        }
    }

    static final class MappingManyToManyStep extends ConfigurableStep implements StepManyToMany<String, String> {
        @Override
        public Multi<String> applyTransform(Multi<String> input) {
            return input.onItem().transform(item -> item + "-mapped");
        }
    }

    static final class BlockingSuffixOneToOneStep extends ConfigurableStep implements StepOneToOneBlocking<String, String> {
        private final String suffix;

        BlockingSuffixOneToOneStep(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String applyBlocking(String in) {
            return in + suffix;
        }
    }

    static final class BlockingExpandingOneToManyStep extends ConfigurableStep implements StepOneToManyBlocking<String, String> {
        @Override
        public List<String> applyBlocking(String in) {
            return List.of(in + "-1", in + "-2");
        }
    }

    static final class BlockingIteratorOneToManyStep extends ConfigurableStep
        implements StepOneToManyBlockingIterator<String, String> {
        @Override
        public CloseableIterator<String> iterateBlocking(String in) {
            return new CloseableIterator<>() {
                private int index;

                @Override
                public boolean hasNext() {
                    return index < 2;
                }

                @Override
                public String next() {
                    return in + "-" + ++index;
                }

                @Override
                public void close() {
                }
            };
        }
    }

    static final class BlockingReducingManyToOneStep extends ConfigurableStep implements StepManyToOneBlocking<String, String> {
        @Override
        public String applyBatchBlocking(List<String> inputs) {
            return String.join(",", inputs);
        }
    }

    static final class BlockingMappingManyToManyStep extends ConfigurableStep implements StepManyToManyBlocking<String, String> {
        @Override
        public List<String> applyBatchBlocking(List<String> inputs) {
            return inputs.stream().map(item -> item + "-mapped").toList();
        }
    }

    record PhysicalOrder(String id) {
    }

    record DigitalOrder(String id) {
    }

    record StockReserved(String id) {
    }

    record ApprovedPaymentStatusMessage(String paymentId) {
    }

    record ApprovedPaymentOutputMessage(String paymentId) {
    }

    static final class PaymentStatusEnvelope {
        private final ApprovedPaymentStatusMessage approved;

        private PaymentStatusEnvelope(ApprovedPaymentStatusMessage approved) {
            this.approved = approved;
        }

        static PaymentStatusEnvelope approved(String paymentId) {
            return new PaymentStatusEnvelope(new ApprovedPaymentStatusMessage(paymentId));
        }

        public boolean hasApproved() {
            return approved != null;
        }

        public ApprovedPaymentStatusMessage getApproved() {
            return approved;
        }
    }

    static final class PaymentOutputBranchEnvelope {
        private final ApprovedPaymentOutputMessage approved;

        private PaymentOutputBranchEnvelope(ApprovedPaymentOutputMessage approved) {
            this.approved = approved;
        }

        public boolean hasApproved() {
            return approved != null;
        }

        public ApprovedPaymentOutputMessage getApproved() {
            return approved;
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static final class Builder {
            private ApprovedPaymentOutputMessage approved;

            public Builder setApproved(ApprovedPaymentOutputMessage approved) {
                this.approved = approved;
                return this;
            }

            public PaymentOutputBranchEnvelope build() {
                return new PaymentOutputBranchEnvelope(approved);
            }
        }
    }

    static final class ReserveStockStep extends ConfigurableStep implements StepOneToOne<PhysicalOrder, StockReserved> {
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public Uni<StockReserved> applyOneToOne(PhysicalOrder in) {
            invocations.incrementAndGet();
            return Uni.createFrom().item(new StockReserved(in.id()));
        }

        int invocations() {
            return invocations.get();
        }
    }

    static final class FinalizePaymentStep extends ConfigurableStep
        implements StepOneToOne<PaymentOutputBranchEnvelope, String> {
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public Uni<String> applyOneToOne(PaymentOutputBranchEnvelope input) {
            invocations.incrementAndGet();
            return Uni.createFrom().item("finalized:" + input.getApproved().paymentId());
        }

        int invocations() {
            return invocations.get();
        }
    }
}
