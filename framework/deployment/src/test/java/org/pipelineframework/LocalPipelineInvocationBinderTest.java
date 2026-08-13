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

package org.pipelineframework;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.awaitable.TerminalOutputOwnership;
import org.pipelineframework.objectpublish.ObjectPayload;
import org.pipelineframework.objectpublish.ObjectPublishMapper;
import org.pipelineframework.objectpublish.ObjectPublishRunner;
import org.pipelineframework.objectpublish.ObjectPublishTelemetry;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectTargetRegistry;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.processor.composition.LocalPipelineInvocationBinder;
import org.pipelineframework.processor.composition.PipelineDefinition;
import org.pipelineframework.processor.composition.PipelineDefinitionLinker;
import org.pipelineframework.processor.composition.PipelineDefinitionStep;
import org.pipelineframework.processor.composition.PipelineInvocationBinding;
import org.pipelineframework.processor.composition.PipelineInvocationRealization;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.composition.ResolvedPipelineDefinitionGraph;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.orchestrator.PipelineExecutionPosition;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.telemetry.PipelineTelemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalPipelineInvocationBinderTest {

    @Mock
    PipelineTelemetry telemetry;

    @Mock
    PipelineStepOrderer stepOrderer;

    @Mock
    PipelineParallelismPolicyResolver parallelismPolicyResolver;

    @Mock
    PipelineCacheSupportFactory cacheSupportFactory;

    private PipelineRunner runner;

    @BeforeEach
    void setUp() {
        runner = new PipelineRunner();
        runner.configFactory = new ConfigFactory();
        runner.pipelineConfig = new PipelineConfig();
        runner.telemetry = telemetry;
        runner.stepOrderer = stepOrderer;
        runner.parallelismPolicyResolver = parallelismPolicyResolver;
        runner.cacheSupportFactory = cacheSupportFactory;
        runner.stepExecutor = new PipelineStepExecutor();

        lenient().when(stepOrderer.orderSteps(any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(parallelismPolicyResolver.resolveParallelismPolicy(any()))
            .thenReturn(ParallelismPolicy.SEQUENTIAL);
        lenient().when(parallelismPolicyResolver.resolveMaxConcurrency(any())).thenReturn(1);
        lenient().when(cacheSupportFactory.buildCacheReadSupport()).thenReturn(null);
        lenient().when(telemetry.startRun(any(), anyInt(), any(), anyInt())).thenReturn(null);
        lenient().when(telemetry.instrumentInput(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(telemetry.instrumentRunCompletion(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(telemetry.instrumentItemConsumed(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentItemProduced(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentItemProduced(any(), any(), any(Uni.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentStepUni(any(), any(), any(), anyBoolean(), any()))
            .thenAnswer(invocation -> invocation.getArgument(1));
    }

    @AfterEach
    void clearContext() {
        PipelineContextHolder.clear();
    }

    @Test
    void localOneToOneInvocationLinksInnerIntoOneRootExecutionAndPublication() throws Exception {
        PipelineReference innerReference = new PipelineReference("inner");
        PipelineDefinition inner = definition(
            innerReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("x", "Text", "Text", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("y", "Text", "Text", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition outer = definition(
            new PipelineReference("outer"),
            "Text",
            "Terminal",
            PipelineDefinitionStep.direct("a", "Text", "Text", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.pipeline("inner", "Text", "Text", innerReference),
            PipelineDefinitionStep.direct("c", "Text", "Terminal", CardinalitySemantics.ONE_TO_ONE));
        ResolvedPipelineDefinitionGraph graph = link(outer, Map.of(innerReference, inner));
        PipelineInvocationBinding binding = graph.invocationBindings().getFirst();
        assertEquals(CardinalitySemantics.ONE_TO_ONE, binding.cardinality());

        AtomicReference<PipelineContext> observedContext = new AtomicReference<>();
        Object innerStep = new LocalPipelineInvocationBinder(
            runner,
            List.of(new ContextSuffix("-x", observedContext), new ContextSuffix("-y", observedContext)))
            .realize(binding);
        assertInstanceOf(StepOneToOne.class, innerStep);

        CountingObjectTargetProvider publisher = new CountingObjectTargetProvider();
        setObjectPublishRunner(new ObjectPublishRunner(
            objectPublishConfig(),
            new ObjectTargetRegistry(List.of(publisher)),
            ObjectPublishTelemetry.NOOP));
        PipelineContext context = new PipelineContext("release-1", "tenant-1", "prefer-cache");
        PipelineContextHolder.set(context);
        AtomicInteger subscriptions = new AtomicInteger();

        PipelineRunner.ExecutionResult execution = runner.runWithContext(
            Uni.createFrom().deferred(() -> {
                subscriptions.incrementAndGet();
                return Uni.createFrom().item("input");
            }),
            List.of(
                new ContextSuffix("-a", observedContext),
                innerStep,
                new TerminalStep(observedContext)));

        assertTrue(execution.terminalOutputPublished());
        assertEquals(0, subscriptions.get(), "linking and nested invocation must not subscribe eagerly");
        assertEquals(new TerminalValue("input-a-x-y-c"),
            ((Uni<TerminalValue>) execution.result()).await().indefinitely());
        assertEquals(1, subscriptions.get());
        assertEquals(1, publisher.writeAttempts());
        assertSame(context, observedContext.get());
        verify(telemetry, times(1)).startRun(any(), anyInt(), any(), anyInt());
    }

    @Test
    void localOneToManyInvocationUsesTheDerivedExistingExpansionInterface() throws Exception {
        PipelineReference innerReference = new PipelineReference("split-inner");
        PipelineDefinition inner = definition(
            innerReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("split", "Text", "Text", CardinalitySemantics.ONE_TO_MANY));
        PipelineDefinition outer = definition(
            new PipelineReference("outer-split"),
            "Text",
            "Text",
            PipelineDefinitionStep.pipeline("split-inner", "Text", "Text", innerReference));
        ResolvedPipelineDefinitionGraph graph = link(outer, Map.of(innerReference, inner));
        PipelineInvocationBinding binding = graph.invocationBindings().getFirst();
        assertEquals(CardinalitySemantics.ONE_TO_MANY, binding.cardinality());

        AtomicReference<PipelineContext> observedContext = new AtomicReference<>();
        Object innerStep = new LocalPipelineInvocationBinder(runner, List.of(new SplitStep(observedContext)))
            .realize(binding);
        assertInstanceOf(StepOneToMany.class, innerStep);
        setObjectPublishRunner(ObjectPublishRunner.disabled());
        PipelineContext context = new PipelineContext("release-2", "tenant-2", "prefer-cache");
        PipelineContextHolder.set(context);
        AtomicInteger subscriptions = new AtomicInteger();

        PipelineRunner.ExecutionResult execution = runner.runWithContext(
            Uni.createFrom().deferred(() -> {
                subscriptions.incrementAndGet();
                return Uni.createFrom().item("red,blue");
            }),
            List.of(innerStep));

        assertFalse(execution.terminalOutputPublished());
        assertEquals(0, subscriptions.get());
        assertEquals(List.of("red", "blue"),
            ((Multi<String>) execution.result()).collect().asList().await().indefinitely());
        assertEquals(1, subscriptions.get());
        assertSame(context, observedContext.get());
        verify(telemetry, times(1)).startRun(any(), anyInt(), any(), anyInt());
    }

    @Test
    void localAggregateInvocationsUseTheExistingReductionAndStreamingInterfaces() throws Exception {
        PipelineReference reductionReference = new PipelineReference("reduce-inner");
        PipelineDefinition reduction = definition(
            reductionReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("join", "Text", "Text", CardinalitySemantics.MANY_TO_ONE));
        PipelineDefinition reductionOuter = definition(
            new PipelineReference("reduce-outer"),
            "Text",
            "Text",
            PipelineDefinitionStep.pipeline("reduce", "Text", "Text", reductionReference));
        PipelineInvocationBinding reductionBinding = link(reductionOuter, Map.of(reductionReference, reduction))
            .invocationBindings()
            .getFirst();
        Object reductionStep = new LocalPipelineInvocationBinder(runner, List.of(new JoinStep()))
            .realize(reductionBinding);
        assertInstanceOf(ManyToOne.class, reductionStep);
        setObjectPublishRunner(ObjectPublishRunner.disabled());

        PipelineRunner.ExecutionResult reductionExecution = runner.runWithContext(
            Multi.createFrom().items("red", "blue"),
            List.of(reductionStep));
        assertEquals("red-blue", ((Uni<String>) reductionExecution.result()).await().indefinitely());
        verify(telemetry, times(1)).startRun(any(), anyInt(), any(), anyInt());

        PipelineReference streamingReference = new PipelineReference("stream-inner");
        PipelineDefinition streaming = definition(
            streamingReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("uppercase", "Text", "Text", CardinalitySemantics.MANY_TO_MANY));
        PipelineDefinition streamingOuter = definition(
            new PipelineReference("stream-outer"),
            "Text",
            "Text",
            PipelineDefinitionStep.pipeline("stream", "Text", "Text", streamingReference));
        PipelineInvocationBinding streamingBinding = link(streamingOuter, Map.of(streamingReference, streaming))
            .invocationBindings()
            .getFirst();
        Object streamingStep = new LocalPipelineInvocationBinder(runner, List.of(new UppercaseStep()))
            .realize(streamingBinding);
        assertInstanceOf(StepManyToMany.class, streamingStep);

        PipelineRunner.ExecutionResult streamingExecution = runner.runWithContext(
            Multi.createFrom().items("red", "blue"),
            List.of(streamingStep));
        assertEquals(List.of("RED", "BLUE"),
            ((Multi<String>) streamingExecution.result()).collect().asList().await().indefinitely());
        verify(telemetry, times(2)).startRun(any(), anyInt(), any(), anyInt());
    }

    @Test
    void pointwiseInvocationOfAnOuterMultiUsesTheExistingMaxConcurrencyBound() throws Exception {
        when(parallelismPolicyResolver.resolveParallelismPolicy(any())).thenReturn(ParallelismPolicy.PARALLEL);
        when(parallelismPolicyResolver.resolveMaxConcurrency(any())).thenReturn(2);
        PipelineReference innerReference = new PipelineReference("bounded-inner");
        PipelineDefinition inner = definition(
            innerReference,
            "Number",
            "Number",
            PipelineDefinitionStep.direct("delay", "Number", "Number", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition outer = definition(
            new PipelineReference("bounded-outer"),
            "Number",
            "Number",
            PipelineDefinitionStep.pipeline("bounded", "Number", "Number", innerReference));
        PipelineInvocationBinding binding = link(outer, Map.of(innerReference, inner)).invocationBindings().getFirst();
        AtomicInteger activeChildren = new AtomicInteger();
        AtomicInteger maximumActiveChildren = new AtomicInteger();
        Object invocation = new LocalPipelineInvocationBinder(
            runner,
            List.of(new DelayedIdentityStep(activeChildren, maximumActiveChildren)))
            .realize(binding);
        setObjectPublishRunner(ObjectPublishRunner.disabled());
        AtomicInteger rootSubscriptions = new AtomicInteger();

        PipelineRunner.ExecutionResult execution = runner.runWithContext(
            Multi.createFrom().deferred(() -> {
                rootSubscriptions.incrementAndGet();
                return Multi.createFrom().range(0, 10);
            }),
            List.of(invocation));

        assertEquals(0, rootSubscriptions.get(), "building an invocation must not subscribe to its outer Multi");
        assertEquals(10, ((Multi<Integer>) execution.result()).collect().asList().await().indefinitely().size());
        assertEquals(1, rootSubscriptions.get());
        assertEquals(2, maximumActiveChildren.get());
        assertEquals(0, activeChildren.get());
    }

    @Test
    void streamScopedChildrenRunOncePerParentStreamWithoutResubscribing() throws Exception {
        setObjectPublishRunner(ObjectPublishRunner.disabled());
        PipelineReference reductionReference = new PipelineReference("scoped-reduction");
        PipelineDefinition reduction = definition(
            reductionReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("join", "Text", "Text", CardinalitySemantics.MANY_TO_ONE));
        PipelineDefinition reductionOuter = definition(
            new PipelineReference("scoped-reduction-outer"),
            "Text",
            "Text",
            PipelineDefinitionStep.pipeline("reduce", "Text", "Text", reductionReference));
        AtomicInteger reductionApplications = new AtomicInteger();
        Object reductionInvocation = new LocalPipelineInvocationBinder(
            runner,
            List.of(new CountingJoinStep(reductionApplications)))
            .realize(link(reductionOuter, Map.of(reductionReference, reduction)).invocationBindings().getFirst());
        AtomicInteger reductionSubscriptions = new AtomicInteger();

        PipelineRunner.ExecutionResult reductionExecution = runner.runWithContext(
            Multi.createFrom().deferred(() -> {
                reductionSubscriptions.incrementAndGet();
                return Multi.createFrom().items("red", "blue");
            }),
            List.of(reductionInvocation));
        assertEquals("red-blue", ((Uni<String>) reductionExecution.result()).await().indefinitely());
        assertEquals(1, reductionApplications.get());
        assertEquals(1, reductionSubscriptions.get());

        PipelineReference streamingReference = new PipelineReference("scoped-streaming");
        PipelineDefinition streaming = definition(
            streamingReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("uppercase", "Text", "Text", CardinalitySemantics.MANY_TO_MANY));
        PipelineDefinition streamingOuter = definition(
            new PipelineReference("scoped-streaming-outer"),
            "Text",
            "Text",
            PipelineDefinitionStep.pipeline("stream", "Text", "Text", streamingReference));
        AtomicInteger streamingApplications = new AtomicInteger();
        Object streamingInvocation = new LocalPipelineInvocationBinder(
            runner,
            List.of(new CountingUppercaseStep(streamingApplications)))
            .realize(link(streamingOuter, Map.of(streamingReference, streaming)).invocationBindings().getFirst());
        AtomicInteger streamingSubscriptions = new AtomicInteger();

        PipelineRunner.ExecutionResult streamingExecution = runner.runWithContext(
            Multi.createFrom().deferred(() -> {
                streamingSubscriptions.incrementAndGet();
                return Multi.createFrom().items("red", "blue");
            }),
            List.of(streamingInvocation));
        assertEquals(List.of("RED", "BLUE"),
            ((Multi<String>) streamingExecution.result()).collect().asList().await().indefinitely());
        assertEquals(1, streamingApplications.get());
        assertEquals(1, streamingSubscriptions.get());
    }

    @Test
    void nestedFailuresAndCancellationUseTheOuterReactiveSubscription() throws Exception {
        setObjectPublishRunner(ObjectPublishRunner.disabled());
        PipelineReference innerReference = new PipelineReference("reactive-inner");
        PipelineDefinition inner = definition(
            innerReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("work", "Text", "Text", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition outer = definition(
            new PipelineReference("reactive-outer"),
            "Text",
            "Text",
            PipelineDefinitionStep.pipeline("inner", "Text", "Text", innerReference));
        PipelineInvocationBinding binding = link(outer, Map.of(innerReference, inner)).invocationBindings().getFirst();
        IllegalStateException failure = new IllegalStateException("inner failure");
        Object failingInvocation = new LocalPipelineInvocationBinder(runner, List.of(new FailingStep(failure)))
            .realize(binding);

        PipelineRunner.ExecutionResult failed = runner.runWithContext(
            Multi.createFrom().item("input"),
            List.of(failingInvocation));
        assertSame(failure, assertThrows(IllegalStateException.class,
            () -> ((Multi<String>) failed.result()).collect().asList().await().indefinitely()));

        CountDownLatch childSubscribed = new CountDownLatch(1);
        CountDownLatch childCancelled = new CountDownLatch(1);
        Object cancellableInvocation = new LocalPipelineInvocationBinder(
            runner,
            List.of(new CancellableStep(childSubscribed, childCancelled)))
            .realize(binding);
        PipelineRunner.ExecutionResult active = runner.runWithContext(
            Multi.createFrom().item("input"),
            List.of(cancellableInvocation));

        var subscription = ((Multi<String>) active.result()).subscribe().with(
            ignored -> fail("The pending child must not emit before cancellation"),
            failureSignal -> fail("The pending child must not fail before cancellation"));
        assertTrue(childSubscribed.await(2, TimeUnit.SECONDS));
        subscription.cancel();
        assertTrue(childCancelled.await(2, TimeUnit.SECONDS));
    }

    @Test
    void bindingIsRealizedByPlacementSpecificAdaptersWithoutEncodingPlacement() {
        PipelineReference innerReference = new PipelineReference("placement-neutral-inner");
        PipelineDefinition inner = definition(
            innerReference,
            "Input",
            "Output",
            PipelineDefinitionStep.direct("step", "Input", "Output", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition outer = definition(
            new PipelineReference("placement-neutral-outer"),
            "Input",
            "Output",
            PipelineDefinitionStep.pipeline("call", "Input", "Output", innerReference));
        PipelineInvocationBinding binding = link(outer, Map.of(innerReference, inner)).invocationBindings().getFirst();

        PipelineInvocationRealization<String> futureRemoteAdapter = current ->
            current.target().logicalId() + ":" + current.cardinality();

        assertEquals("placement-neutral-inner:ONE_TO_ONE", futureRemoteAdapter.realize(binding));
    }

    @Test
    void nestedAwaitPositionResumesTheInnerSuffixWithoutRestartingTheChild() throws Exception {
        setObjectPublishRunner(ObjectPublishRunner.disabled());
        PipelineReference innerReference = new PipelineReference("inner-await");
        PipelineDefinition inner = definition(
            innerReference,
            "Text",
            "Text",
            PipelineDefinitionStep.direct("x", "Text", "Text", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("await", "Text", "Text", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("y", "Text", "Text", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition outer = definition(
            new PipelineReference("outer-await"),
            "Text",
            "Text",
            PipelineDefinitionStep.direct("a", "Text", "Text", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.pipeline("invoke", "Text", "Text", innerReference),
            PipelineDefinitionStep.direct("c", "Text", "Text", CardinalitySemantics.ONE_TO_ONE));
        PipelineInvocationBinding binding = link(outer, Map.of(innerReference, inner)).invocationBindings().getFirst();
        AtomicInteger innerPrefixApplications = new AtomicInteger();
        Object invocation = new LocalPipelineInvocationBinder(runner, List.of(
            new CountingSuffix("-x", innerPrefixApplications),
            new SuspendingStep(),
            new ContextSuffix("-y", new AtomicReference<>())))
            .realize(binding);
        List<Object> outerSteps = List.of(
            new ContextSuffix("-a", new AtomicReference<>()), invocation,
            new ContextSuffix("-c", new AtomicReference<>()));

        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            "tenant-await", "execution-await", 1,
            org.pipelineframework.awaitable.AwaitContinuationMode.DURABLE_HANDOFF,
            TerminalOutputOwnership.COORDINATOR));
        AwaitSuspendedException suspended;
        try {
            suspended = assertThrows(AwaitSuspendedException.class, () ->
                ((Uni<String>) runner.runFromStep(Uni.createFrom().item("input-a"), outerSteps, 1))
                    .await().indefinitely());
        } finally {
            AwaitExecutionContextHolder.clear();
        }

        PipelineExecutionPosition waiting = suspended.position();
        assertEquals(1, waiting.rootStepIndex());
        assertEquals(binding.childStepLocations().get(1).display(), waiting.staticLocation());
        assertEquals(binding.childStepLocations().get(2).display(), waiting.nextStaticLocation());
        assertEquals(1, innerPrefixApplications.get());

        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            "tenant-await", "execution-await", waiting.next(),
            org.pipelineframework.awaitable.AwaitContinuationMode.DURABLE_HANDOFF,
            TerminalOutputOwnership.COORDINATOR));
        try {
            assertEquals("input-a-x-y-c", ((Uni<String>) runner.runFromStep(
                Uni.createFrom().item("input-a-x"), outerSteps, 1)).await().indefinitely());
        } finally {
            AwaitExecutionContextHolder.clear();
        }
        assertEquals(1, innerPrefixApplications.get(), "recovery must enter the linked inner suffix, not step zero");
    }

    private static ResolvedPipelineDefinitionGraph link(
        PipelineDefinition root,
        Map<PipelineReference, PipelineDefinition> localDefinitions
    ) {
        return new PipelineDefinitionLinker(reference -> Optional.ofNullable(localDefinitions.get(reference))).link(root);
    }

    private static PipelineDefinition definition(
        PipelineReference reference,
        String input,
        String output,
        PipelineDefinitionStep... steps
    ) {
        return new PipelineDefinition(reference, input, output, List.of(steps));
    }

    private static PipelineYamlConfig objectPublishConfig() {
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
            "terminal",
            "object",
            "counting",
            Map.of(),
            new PipelineObjectNamingConfig("{groupKey}"),
            PipelineObjectPublishPayloadConfig.defaults());
        return new PipelineYamlConfig(
            "org.pipelineframework",
            "LOCAL",
            "COMPUTE",
            List.of(),
            Map.of(),
            Map.of(),
            Map.of("terminal", target),
            List.of(),
            null,
            new PipelineOutputBoundaryConfig(null, new PipelineObjectOutputConfig(
                "terminal",
                TerminalValue.class.getName(),
                "TerminalValue",
                TerminalMapper.class.getName())));
    }

    private void setObjectPublishRunner(ObjectPublishRunner publishRunner) throws Exception {
        Field field = PipelineRunner.class.getDeclaredField("objectPublishRunner");
        field.setAccessible(true);
        field.set(runner, publishRunner);
    }

    record TerminalValue(String value) {
    }

    public static final class TerminalMapper implements ObjectPublishMapper<TerminalValue> {
        @Override
        public String groupKey(TerminalValue item) {
            return "result";
        }

        @Override
        public ObjectPayload render(String groupKey, List<TerminalValue> items) {
            return new ObjectPayload(items.getFirst().value().getBytes(StandardCharsets.UTF_8), "text/plain", Map.of());
        }
    }

    private static final class CountingObjectTargetProvider implements ObjectTargetProvider {
        private final AtomicInteger writeAttempts = new AtomicInteger();

        @Override
        public String providerName() {
            return "counting";
        }

        @Override
        public CompletionStage<ObjectWriteResult> write(ObjectWriteRequest request) {
            writeAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(new ObjectWriteResult(
                new PayloadReference(
                    providerName(),
                    "test",
                    request.objectKey(),
                    "application/octet-stream",
                    "raw",
                    request.checksum(),
                    request.bytes().length,
                    "test",
                    Map.of()),
                request.bytes().length,
                request.checksum(),
                null));
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("Batch publish uses write"));
        }

        private int writeAttempts() {
            return writeAttempts.get();
        }
    }

    private static final class ContextSuffix extends ConfigurableStep implements StepOneToOne<String, String> {
        private final String suffix;
        private final AtomicReference<PipelineContext> observedContext;

        private ContextSuffix(String suffix, AtomicReference<PipelineContext> observedContext) {
            this.suffix = suffix;
            this.observedContext = observedContext;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            observedContext.set(PipelineContextHolder.get());
            return Uni.createFrom().item(input + suffix);
        }
    }

    private static final class CountingSuffix extends ConfigurableStep implements StepOneToOne<String, String> {
        private final String suffix;
        private final AtomicInteger applications;

        private CountingSuffix(String suffix, AtomicInteger applications) {
            this.suffix = suffix;
            this.applications = applications;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            applications.incrementAndGet();
            return Uni.createFrom().item(input + suffix);
        }
    }

    private static final class SuspendingStep extends ConfigurableStep implements StepOneToOne<String, String> {
        @Override
        public Uni<String> applyOneToOne(String input) {
            AwaitExecutionContext context = AwaitExecutionContextHolder.get();
            return Uni.createFrom().failure(new AwaitSuspendedException(
                context.tenantId(), context.executionId(), "unit-await", context.currentPosition()));
        }
    }

    private static final class TerminalStep extends ConfigurableStep implements StepOneToOne<String, TerminalValue> {
        private final AtomicReference<PipelineContext> observedContext;

        private TerminalStep(AtomicReference<PipelineContext> observedContext) {
            this.observedContext = observedContext;
        }

        @Override
        public Uni<TerminalValue> applyOneToOne(String input) {
            observedContext.set(PipelineContextHolder.get());
            return Uni.createFrom().item(new TerminalValue(input + "-c"));
        }
    }

    private static final class SplitStep extends ConfigurableStep implements StepOneToMany<String, String> {
        private final AtomicReference<PipelineContext> observedContext;

        private SplitStep(AtomicReference<PipelineContext> observedContext) {
            this.observedContext = observedContext;
        }

        @Override
        public Multi<String> applyOneToMany(String input) {
            observedContext.set(PipelineContextHolder.get());
            return Multi.createFrom().items(input.split(","));
        }
    }

    private static final class JoinStep extends ConfigurableStep implements ManyToOne<String, String> {
        @Override
        public Uni<String> apply(Multi<String> input) {
            return input.collect().asList().map(items -> String.join("-", items));
        }
    }

    private static final class UppercaseStep extends ConfigurableStep implements StepManyToMany<String, String> {
        @Override
        public Multi<String> applyTransform(Multi<String> input) {
            return input.onItem().transform(String::toUpperCase);
        }
    }

    private static final class DelayedIdentityStep extends ConfigurableStep implements StepOneToOne<Integer, Integer> {
        private final AtomicInteger active;
        private final AtomicInteger maximumActive;

        private DelayedIdentityStep(AtomicInteger active, AtomicInteger maximumActive) {
            this.active = active;
            this.maximumActive = maximumActive;
        }

        @Override
        public Uni<Integer> applyOneToOne(Integer input) {
            return Uni.createFrom().deferred(() -> {
                int current = active.incrementAndGet();
                maximumActive.accumulateAndGet(current, Math::max);
                return Uni.createFrom().item(input)
                    .onItem().delayIt().by(Duration.ofMillis(20))
                    .onTermination().invoke(active::decrementAndGet);
            });
        }
    }

    private static final class CountingJoinStep extends ConfigurableStep implements ManyToOne<String, String> {
        private final AtomicInteger applications;

        private CountingJoinStep(AtomicInteger applications) {
            this.applications = applications;
        }

        @Override
        public Uni<String> apply(Multi<String> input) {
            applications.incrementAndGet();
            return input.collect().asList().map(items -> String.join("-", items));
        }
    }

    private static final class CountingUppercaseStep extends ConfigurableStep implements StepManyToMany<String, String> {
        private final AtomicInteger applications;

        private CountingUppercaseStep(AtomicInteger applications) {
            this.applications = applications;
        }

        @Override
        public Multi<String> applyTransform(Multi<String> input) {
            applications.incrementAndGet();
            return input.onItem().transform(String::toUpperCase);
        }
    }

    private static final class FailingStep extends ConfigurableStep implements StepOneToOne<String, String> {
        private final IllegalStateException failure;

        private FailingStep(IllegalStateException failure) {
            this.failure = failure;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom().failure(failure);
        }
    }

    private static final class CancellableStep extends ConfigurableStep implements StepOneToOne<String, String> {
        private final CountDownLatch subscribed;
        private final CountDownLatch cancelled;

        private CancellableStep(CountDownLatch subscribed, CountDownLatch cancelled) {
            this.subscribed = subscribed;
            this.cancelled = cancelled;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom().emitter(emitter -> {
                subscribed.countDown();
                emitter.onTermination(cancelled::countDown);
            });
        }
    }
}
