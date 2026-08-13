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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
import org.pipelineframework.invocation.PipelineInvocationSteps;
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
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.composition.ResolvedPipelineDefinitionGraph;
import org.pipelineframework.repository.PayloadReference;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
        Object innerStep = new LocalPipelineInvocationBinder().bind(
            binding,
            runner,
            List.of(new ContextSuffix("-x", observedContext), new ContextSuffix("-y", observedContext)));
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
        Object innerStep = new LocalPipelineInvocationBinder().bind(
            binding,
            runner,
            List.of(new SplitStep(observedContext)));
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
        Object reductionStep = new LocalPipelineInvocationBinder().bind(
            reductionBinding,
            runner,
            List.of(new JoinStep()));
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
        Object streamingStep = new LocalPipelineInvocationBinder().bind(
            streamingBinding,
            runner,
            List.of(new UppercaseStep()));
        assertInstanceOf(StepManyToMany.class, streamingStep);

        PipelineRunner.ExecutionResult streamingExecution = runner.runWithContext(
            Multi.createFrom().items("red", "blue"),
            List.of(streamingStep));
        assertEquals(List.of("RED", "BLUE"),
            ((Multi<String>) streamingExecution.result()).collect().asList().await().indefinitely());
        verify(telemetry, times(2)).startRun(any(), anyInt(), any(), anyInt());
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
}
