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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.file.Files;
import java.nio.file.Path;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.branching.StepBranchingDescriptor;
import org.pipelineframework.branching.BranchVariantIdentity;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepManyToOne;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.cache.PipelineCacheReader;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.FilePipelineReplayExporter;
import org.pipelineframework.telemetry.PipelineExecutionEvent;
import org.pipelineframework.telemetry.PipelineReplayDocument;
import org.pipelineframework.telemetry.PipelineReplayExporter;
import org.pipelineframework.telemetry.PipelineReplayRunParameters;
import org.pipelineframework.telemetry.PipelineReplayTopology;
import org.pipelineframework.telemetry.PipelineTelemetry;
import org.pipelineframework.telemetry.RetryAmplificationGuardMode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineReplayExecutionTest {

    @TempDir
    Path tempDir;

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
        metricReader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(sdk);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.shutdown();
        meterProvider.shutdown();
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void emitsReplayEventsForOneToOneOneToManyAndManyToOne() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, topology());

        Payload inputPayload = new Payload("alpha");
        Multi<Payload> input = Multi.createFrom().item(inputPayload);
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 3, ParallelismPolicy.AUTO, 4);

        Object current = input;
        current = PipelineStepExecutor.applyOneToOneUnchecked(new PrefixStep(), current, false, 4, telemetry, runContext, null, null);
        current = PipelineStepExecutor.applyOneToManyUnchecked(new SplitStep(), current, false, 4, telemetry, runContext, null);
        current = PipelineStepExecutor.applyManyToOneUnchecked(new MergeStep(), current, telemetry, runContext, null);
        Uni<Payload> completed = (Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext);

        Payload output = completed.await().indefinitely();

        assertEquals("pref-alpha|pref-alpha-a+pref-alpha|pref-alpha-b", output.value());
        assertTrue(exporter.events.stream().anyMatch(event -> "start".equals(event.event())));
        assertTrue(exporter.events.stream().anyMatch(event -> "emit".equals(event.event())));
        assertTrue(exporter.events.stream().anyMatch(event -> "success".equals(event.event())));
        assertNotNull(exporter.runParameters);
        assertEquals("auto", findParameterValue(exporter.runParameters, "pipeline.parallelism"));
        assertEquals("4", findParameterValue(exporter.runParameters, "pipeline.max-concurrency"));
        assertEquals("prefer-cache", findParameterValue(exporter.runParameters, "pipeline.cache.policy"));
        assertEquals("file", findParameterValue(exporter.runParameters, "pipeline.telemetry.replay.exporter"));
        assertEquals("log", findParameterValue(exporter.runParameters, "pipeline.item-reject.provider"));

        List<PipelineExecutionEvent> fanOut = exporter.events.stream()
            .filter(event -> "emit".equals(event.event()) && "Split".equals(event.step()))
            .toList();
        assertEquals(2, fanOut.size());
        assertTrue(fanOut.stream().allMatch(event -> event.parentItemIds() != null && event.parentItemIds().size() == 1));

        PipelineExecutionEvent mergeEmit = exporter.events.stream()
            .filter(event -> "emit".equals(event.event()) && "Merge".equals(event.step()))
            .findFirst()
            .orElseThrow();
        assertNotNull(mergeEmit.parentItemIds());
        assertEquals(2, mergeEmit.parentItemIds().size());
        assertTrue(mergeEmit.parentItemIds().contains(fanOut.get(0).itemId()));
        assertTrue(mergeEmit.parentItemIds().contains(fanOut.get(1).itemId()));
    }

    @Test
    void emitsReplayEventsForManyToMany() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, manyToManyTopology());

        Multi<Payload> input = Multi.createFrom().items(new Payload("alpha"), new Payload("beta"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);

        Object current = PipelineStepExecutor.applyManyToManyUnchecked(
            new TransformStep(), input, telemetry, runContext, null);
        List<Payload> output = ((Multi<Payload>) telemetry.instrumentRunCompletion(current, runContext))
            .collect().asList().await().indefinitely();

        assertEquals(List.of(new Payload("transformed-alpha"), new Payload("transformed-beta")), output);
        assertEquals(1, exporter.events.stream()
            .filter(event -> "start".equals(event.event()) && "Transform".equals(event.step())).count());
        assertEquals(1, exporter.events.stream()
            .filter(event -> "success".equals(event.event()) && "Transform".equals(event.step())).count());
        List<PipelineExecutionEvent> emitted = exporter.events.stream()
            .filter(event -> "emit".equals(event.event()) && "Transform".equals(event.step()))
            .toList();
        assertEquals(2, emitted.size());
    }

    @Test
    void emitsRetryEventWithAttemptCount() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, retryTopology());

        Uni<Payload> input = Uni.createFrom().item(new Payload("beta"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new RetryOnceStep(), input, false, 4, telemetry, runContext, null, null);
        Uni<Payload> completed = (Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext);

        Payload output = completed.await().indefinitely();

        assertEquals("retry-beta", output.value());
        PipelineExecutionEvent retry = exporter.events.stream()
            .filter(event -> "retry".equals(event.event()))
            .findFirst()
            .orElseThrow();
        assertEquals(1, retry.attempt());
        assertEquals(IllegalStateException.class.getName(), retry.errorType());
    }

    @Test
    void flushesReplayStateOnTerminalFailure() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, failureTopology());

        Uni<Payload> input = Uni.createFrom().item(new Payload("gamma"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new AlwaysFailStep(), input, false, 4, telemetry, runContext, null, null);
        Uni<Payload> completed = (Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext);

        IllegalArgumentException failure =
            assertThrows(IllegalArgumentException.class, () -> completed.await().indefinitely());

        assertNotNull(failure);
        assertEquals("failed", exporter.status);
        assertEquals(IllegalArgumentException.class.getName(), exporter.failureType);
        assertEquals("boom", exporter.failureMessage);
        assertTrue(exporter.durationMs >= 0L);
        assertTrue(exporter.events.stream().anyMatch(event -> "start".equals(event.event())));
        assertTrue(exporter.events.stream().anyMatch(event -> "error".equals(event.event())));
    }

    @Test
    void flushesReplayStateWhenRunIsAbortedExternally() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, failureTopology());

        Uni<Payload> input = Uni.createFrom().item(new Payload("delta"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new NeverCompletesStep(), input, false, 4, telemetry, runContext, null, null);
        telemetry.instrumentRunCompletion(current, runContext);

        telemetry.abortActiveRun(new PipelineKillSwitchException("kill", "retry_amplification", "global"));
        telemetry.abortActiveRun(new PipelineKillSwitchException("kill", "retry_amplification", "global"));

        assertEquals("failed", exporter.status);
        assertEquals(PipelineKillSwitchException.class.getName(), exporter.failureType);
        assertEquals("kill", exporter.failureMessage);
        assertTrue(exporter.durationMs >= 0L);
    }

    @Test
    void fileReplayExporterWritesReplayJsonOnSuccess() throws Exception {
        Path output = tempDir.resolve("success-replay.json");
        PipelineTelemetry telemetry = new PipelineTelemetry(
            new ReplayEnabledPipelineStepConfig(output.toString()),
            new FilePipelineReplayExporter(output),
            topology());

        Payload inputPayload = new Payload("alpha");
        Multi<Payload> input = Multi.createFrom().item(inputPayload);
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 3, ParallelismPolicy.AUTO, 4);

        Object current = input;
        current = PipelineStepExecutor.applyOneToOneUnchecked(new PrefixStep(), current, false, 4, telemetry, runContext, null, null);
        current = PipelineStepExecutor.applyOneToManyUnchecked(new SplitStep(), current, false, 4, telemetry, runContext, null);
        current = PipelineStepExecutor.applyManyToOneUnchecked(new MergeStep(), current, telemetry, runContext, null);
        ((Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext)).await().indefinitely();

        assertTrue(Files.isRegularFile(output));
        PipelineReplayDocument document = PipelineJson.mapper().readValue(output.toFile(), PipelineReplayDocument.class);
        assertEquals("completed", document.status());
        assertNotNull(document.runParameters());
        assertFalse(document.runParameters().sections().isEmpty());
        assertTrue(document.events().stream().anyMatch(event -> "success".equals(event.event())));
    }

    @Test
    void fileReplayExporterWritesReplayJsonOnAbort() throws Exception {
        Path output = tempDir.resolve("failed-replay.json");
        PipelineTelemetry telemetry = new PipelineTelemetry(
            new ReplayEnabledPipelineStepConfig(output.toString()),
            new FilePipelineReplayExporter(output),
            failureTopology());

        Uni<Payload> input = Uni.createFrom().item(new Payload("delta"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new NeverCompletesStep(), input, false, 4, telemetry, runContext, null, null);
        telemetry.instrumentRunCompletion(current, runContext);

        telemetry.abortActiveRun(new PipelineKillSwitchException("kill", "retry_amplification", "global"));

        assertTrue(Files.isRegularFile(output));
        PipelineReplayDocument document = PipelineJson.mapper().readValue(output.toFile(), PipelineReplayDocument.class);
        assertEquals("failed", document.status());
        assertEquals(PipelineKillSwitchException.class.getName(), document.failureType());
        assertEquals("kill", document.failureMessage());
        assertNotNull(document.runParameters());
        assertFalse(document.runParameters().sections().isEmpty());
    }

    @Test
    void fileReplayExporterWritesOneReplayFilePerRunWhenConfiguredPathIsDirectory() throws Exception {
        Path outputDir = tempDir.resolve("replay-runs");
        PipelineTelemetry telemetry = new PipelineTelemetry(
            new ReplayEnabledPipelineStepConfig(outputDir.toString()),
            new FilePipelineReplayExporter(outputDir),
            topology());

        for (String value : List.of("alpha", "beta")) {
            Multi<Payload> input = Multi.createFrom().item(new Payload(value));
            PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
            Object current = PipelineStepExecutor.applyOneToOneUnchecked(
                new PrefixStep(), input, false, 4, telemetry, runContext, null, null);
            ((Multi<Payload>) telemetry.instrumentRunCompletion(current, runContext)).collect().asList().await().indefinitely();
        }

        List<Path> replayFiles;
        try (var replayFileStream = Files.list(outputDir)) {
            replayFiles = replayFileStream
                .filter(path -> path.getFileName().toString().endsWith(".json"))
                .sorted()
                .toList();
        }
        assertEquals(2, replayFiles.size());
        for (Path replayFile : replayFiles) {
            PipelineReplayDocument document = PipelineJson.mapper().readValue(replayFile.toFile(), PipelineReplayDocument.class);
            assertEquals("completed", document.status());
            assertTrue(document.events().stream().anyMatch(event -> "success".equals(event.event())));
        }
    }

    @Test
    void fileReplayExporterWritesAwaitLifecycleControlFragmentInDirectoryMode() throws Exception {
        Path outputDir = tempDir.resolve("await-control-runs");
        PipelineTelemetry telemetry = new PipelineTelemetry(
            new ReplayEnabledPipelineStepConfig(outputDir.toString()),
            new FilePipelineReplayExporter(outputDir),
            awaitTopology());

        telemetry.recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            AwaitReplayLifecycleEvent.UNIT_ITEM_COMPLETED,
            "exec-1",
            "unit-1",
            "AwaitProvider",
            1,
            "WAITING_EXTERNAL",
            "interaction-1",
            "correlation-1",
            "kafka",
            0,
            2,
            1,
            true));

        List<Path> replayFiles;
        try (var replayFileStream = Files.list(outputDir)) {
            replayFiles = replayFileStream
                .filter(path -> path.getFileName().toString().endsWith(".json"))
                .sorted()
                .toList();
        }
        assertEquals(1, replayFiles.size());
        PipelineReplayDocument document = PipelineJson.mapper().readValue(replayFiles.getFirst().toFile(), PipelineReplayDocument.class);
        assertEquals("completed", document.status());
        PipelineExecutionEvent event = document.events().getFirst();
        assertEquals(AwaitReplayLifecycleEvent.UNIT_ITEM_COMPLETED, event.event());
        assertEquals("AwaitProvider", event.step());
        assertEquals("unit-1", event.attributes().get("tpf.await.unit_id"));
        assertEquals("interaction-1", event.attributes().get("tpf.await.interaction_id"));
        assertEquals("2", event.attributes().get("tpf.await.expected_item_count"));
        assertEquals("1", event.attributes().get("tpf.await.completed_item_count"));
    }

    @Test
    void replayStaysColdWhenReplayExportIsDisabled() {
        Path output = tempDir.resolve("disabled-replay.json");
        PipelineTelemetry telemetry = new PipelineTelemetry(
            new ReplayDisabledPipelineStepConfig(output.toString()),
            new FilePipelineReplayExporter(output),
            topology());

        Uni<Payload> input = Uni.createFrom().item(new Payload("zeta"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new PrefixStep(), input, false, 4, telemetry, runContext, null, null);
        ((Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext)).await().indefinitely();

        assertFalse(Files.exists(output));
        assertNotNull(runContext);
        assertEquals(null, runContext.replayState());
    }

    @Test
    void replayStaysColdWhenPrerequisitesAreMissing() {
        Path output = tempDir.resolve("misconfigured-replay.json");
        PipelineTelemetry telemetry = new PipelineTelemetry(
            new ReplayMissingPrerequisitesPipelineStepConfig(output.toString()),
            new FilePipelineReplayExporter(output),
            topology());

        Uni<Payload> input = Uni.createFrom().item(new Payload("eta"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new PrefixStep(), input, false, 4, telemetry, runContext, null, null);
        ((Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext)).await().indefinitely();

        assertFalse(Files.exists(output));
        assertEquals(null, runContext.replayState());
    }

    @Test
    void emitsCacheHitReplayEventAndPreservesOrdinaryStepEvents() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, cacheHitTopology());
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            new PipelineCacheReader() {
                @Override
                public Uni<Optional<Object>> get(String key) {
                    return Uni.createFrom().item(Optional.of(new Payload("cached-alpha")));
                }

                @Override
                public Uni<Boolean> exists(String key) {
                    return Uni.createFrom().item(true);
                }
            },
            List.<CacheKeyStrategy>of((item, context) -> Optional.of("key")),
            "prefer-cache");
        PipelineContext context = new PipelineContext("v1", null, "prefer-cache");

        Uni<Payload> input = Uni.createFrom().item(new Payload("alpha"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new PrefixStep(), input, false, 4, telemetry, runContext, support, context);
        Payload output = ((Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext)).await().indefinitely();

        assertEquals("cached-alpha", output.value());
        assertTrue(exporter.events.stream().anyMatch(event -> "cache_hit".equals(event.event())));
        assertTrue(exporter.events.stream().anyMatch(event -> "start".equals(event.event())));
        assertTrue(exporter.events.stream().anyMatch(event -> "success".equals(event.event())));
    }

    @Test
    void emitsRejectReplayEventAndAugmentsTopologyWithRejectNode() throws Exception {
        Path output = tempDir.resolve("reject-replay.json");
        PipelineTelemetry telemetry = new PipelineTelemetry(
            new ReplayEnabledPipelineStepConfig(output.toString()),
            new FilePipelineReplayExporter(output),
            rejectTopology());

        Uni<Payload> input = Uni.createFrom().item(new Payload("reject-me"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new RejectingStep(), input, false, 4, telemetry, runContext, null, null);
        ((Uni<Payload>) telemetry.instrumentRunCompletion(current, runContext)).await().indefinitely();

        PipelineReplayDocument document = PipelineJson.mapper().readValue(output.toFile(), PipelineReplayDocument.class);
        assertTrue(document.events().stream().anyMatch(event -> "reject".equals(event.event())));
        assertTrue(document.topology().steps().stream()
            .anyMatch(step -> "reject".equals(step.pluginKind()) && step.step().equals("Rejects Rejecting")));
    }

    @Test
    void emitsSkipReplayEventForNonApplicableBranchStep() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, branchSkipTopology());

        Uni<DigitalPayload> input = Uni.createFrom().item(new DigitalPayload("digital-1"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            0,
            "PhysicalOnly",
            PhysicalOnlyStep.class.getName(),
            PhysicalPayload.class.getName(),
            PhysicalPayload.class,
            List.of("PhysicalPayload"),
            List.of(PhysicalPayload.class.getName()),
            List.of(PhysicalPayload.class),
            false);

        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new PhysicalOnlyStep(),
            input,
            false,
            4,
            telemetry,
            runContext,
            null,
            null,
            null,
            descriptor);
        Object output = ((Uni<?>) telemetry.instrumentRunCompletion(current, runContext)).await().indefinitely();

        assertEquals(new DigitalPayload("digital-1"), output);
        PipelineExecutionEvent skip = exporter.events.stream()
            .filter(event -> "skip".equals(event.event()))
            .findFirst()
            .orElseThrow();
        assertEquals("PhysicalOnly", skip.step());
        assertEquals("not_applicable", skip.attributes().get("reason"));
        assertEquals("PhysicalPayload", skip.attributes().get("acceptedTypes"));
        assertTrue(exporter.events.stream().noneMatch(event ->
            "start".equals(event.event()) && "PhysicalOnly".equals(event.step())));
        assertTrue(exporter.events.stream().noneMatch(event ->
            "success".equals(event.event()) && "PhysicalOnly".equals(event.step())));
    }

    @Test
    void recordsDeclaredOneofIdentityOnBranchSkipReplayEvents() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, branchSkipTopology());
        Uni<PaymentStatusEnvelope> input = Uni.createFrom().item(PaymentStatusEnvelope.digital("digital-1"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            0,
            "PhysicalOnly",
            PhysicalOnlyStep.class.getName(),
            PhysicalPayload.class.getName(),
            PhysicalPayload.class,
            List.of("PhysicalPayload"),
            List.of(PhysicalPayload.class.getName()),
            List.of(PhysicalPayload.class),
            List.of(new BranchVariantIdentity("PaymentStatus", "digital", "DigitalPayload")),
            List.of(),
            List.of(),
            false);

        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new PhysicalOnlyStep(), input, false, 4, telemetry, runContext, null, null, null, descriptor);
        ((Uni<?>) telemetry.instrumentRunCompletion(current, runContext)).await().indefinitely();

        PipelineExecutionEvent skip = exporter.events.stream()
            .filter(event -> "skip".equals(event.event()))
            .findFirst()
            .orElseThrow();
        assertEquals("PaymentStatus", skip.attributes().get("unionName"));
        assertEquals("digital", skip.attributes().get("discriminator"));
        assertEquals("DigitalPayload", skip.attributes().get("payloadContract"));
    }

    @Test
    void recordsDeclaredOneofIdentityForMultiBranchSkips() {
        CollectingExporter exporter = new CollectingExporter();
        PipelineTelemetry telemetry = new PipelineTelemetry(new ReplayEnabledPipelineStepConfig(), exporter, branchSkipTopology());
        Multi<PaymentStatusEnvelope> input = Multi.createFrom().items(
            PaymentStatusEnvelope.digital("digital-1"), PaymentStatusEnvelope.digital("digital-2"));
        PipelineTelemetry.RunContext runContext = telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            0, "PhysicalOnly", PhysicalOnlyStep.class.getName(), PhysicalPayload.class.getName(), PhysicalPayload.class,
            List.of("PhysicalPayload"), List.of(PhysicalPayload.class.getName()), List.of(PhysicalPayload.class),
            List.of(new BranchVariantIdentity("PaymentStatus", "digital", "DigitalPayload")), List.of(), List.of(), false);

        Object current = PipelineStepExecutor.applyOneToOneUnchecked(
            new PhysicalOnlyStep(), input, false, 4, telemetry, runContext, null, null, null, descriptor);
        ((Multi<?>) telemetry.instrumentRunCompletion(current, runContext)).collect().asList().await().indefinitely();

        List<PipelineExecutionEvent> skips = exporter.events.stream()
            .filter(event -> "skip".equals(event.event()))
            .toList();
        assertEquals(2, skips.size());
        assertTrue(skips.stream().allMatch(skip ->
            "PaymentStatus".equals(skip.attributes().get("unionName"))
                && "digital".equals(skip.attributes().get("discriminator"))
                && "DigitalPayload".equals(skip.attributes().get("payloadContract"))));
    }

    private PipelineReplayTopology topology() {
        String step1 = PrefixStep.class.getName();
        String step2 = SplitStep.class.getName();
        String step3 = MergeStep.class.getName();
        return new PipelineReplayTopology(
            "csv-payments",
            List.of(
                new PipelineReplayTopology.Step(step1, "Prefix", "PrefixService", "one-to-one", 0, false, null, null),
                new PipelineReplayTopology.Step(step2, "Split", "SplitService", "one-to-many", 1, false, null, null),
                new PipelineReplayTopology.Step(step3, "Merge", "MergeService", "many-to-one", 2, false, null, null)
            ),
            List.of(
                new PipelineReplayTopology.Transition("Prefix->Split", step1, step2, "Prefix", "Split",
                    "PrefixService", "SplitService", "one-to-one"),
                new PipelineReplayTopology.Transition("Split->Merge", step2, step3, "Split", "Merge",
                    "SplitService", "MergeService", "one-to-many")
            ));
    }

    private PipelineReplayTopology manyToManyTopology() {
        String step = TransformStep.class.getName();
        return new PipelineReplayTopology(
            "csv-payments",
            List.of(new PipelineReplayTopology.Step(step, "Transform", "TransformService", "many-to-many", 0, false, null, null)),
            List.of());
    }

    private PipelineReplayTopology retryTopology() {
        String step = RetryOnceStep.class.getName();
        return new PipelineReplayTopology(
            "csv-payments",
            List.of(new PipelineReplayTopology.Step(step, "RetryOnce", "RetryOnceService", "one-to-one", 0, false, null, null)),
            List.of());
    }

    private PipelineReplayTopology failureTopology() {
        String step = AlwaysFailStep.class.getName();
        return new PipelineReplayTopology(
            "csv-payments",
            List.of(new PipelineReplayTopology.Step(step, "AlwaysFail", "AlwaysFailService", "one-to-one", 0, false, null, null)),
            List.of());
    }

    private PipelineReplayTopology cacheHitTopology() {
        String step = PrefixStep.class.getName();
        String cacheStep = "org.pipelineframework.synthetic.CachePrefixClientStep";
        return new PipelineReplayTopology(
            "search",
            List.of(
                new PipelineReplayTopology.Step(step, "Prefix", "PrefixService", "one-to-one", 0, false, null, null),
                new PipelineReplayTopology.Step(cacheStep, "ObserveCachePrefix", "ObserveCachePrefixService", "one-to-one", 1, true,
                    "Prefix", "cache")
            ),
            List.of(new PipelineReplayTopology.Transition(
                "Prefix->ObserveCachePrefix",
                step,
                cacheStep,
                "Prefix",
                "ObserveCachePrefix",
                "PrefixService",
                "ObserveCachePrefixService",
                "one-to-one")));
    }

    private PipelineReplayTopology rejectTopology() {
        String step = RejectingStep.class.getName();
        return new PipelineReplayTopology(
            "search",
            List.of(new PipelineReplayTopology.Step(step, "Rejecting", "RejectingService", "one-to-one", 0, false, null, null)),
            List.of());
    }

    private PipelineReplayTopology awaitTopology() {
        String inputStep = "org.pipelineframework.test.InputStep";
        String awaitStep = "org.pipelineframework.test.AwaitProviderStep";
        String resumedStep = "org.pipelineframework.test.ResumedStep";
        return new PipelineReplayTopology(
            "csv-payments",
            List.of(
                new PipelineReplayTopology.Step(inputStep, "Input", "InputService", "one-to-one", 0, false, null, null, "primary", null),
                new PipelineReplayTopology.Step(awaitStep, "AwaitProvider", "AwaitProviderService", "one-to-one", 1, false, null, null, "await", null),
                new PipelineReplayTopology.Step(resumedStep, "Resumed", "ResumedService", "one-to-one", 2, false, null, null, "primary", null)
            ),
            List.of(
                new PipelineReplayTopology.Transition("Input->AwaitProvider", inputStep, awaitStep, "Input", "AwaitProvider",
                    "InputService", "AwaitProviderService", "one-to-one", "primary"),
                new PipelineReplayTopology.Transition("AwaitProvider->Resumed", awaitStep, resumedStep, "AwaitProvider", "Resumed",
                    "AwaitProviderService", "ResumedService", "one-to-one", "primary")
            ));
    }

    private PipelineReplayTopology branchSkipTopology() {
        String step = PhysicalOnlyStep.class.getName();
        return new PipelineReplayTopology(
            "order-routing",
            List.of(new PipelineReplayTopology.Step(step, "PhysicalOnly", "PhysicalOnlyService", "one-to-one", 0, false, null, null)),
            List.of());
    }

    record Payload(String value) {
    }

    record PhysicalPayload(String id) {
    }

    record PhysicalHandled(String id) {
    }

    record DigitalPayload(String id) {
    }

    static final class PaymentStatusEnvelope {
        private final DigitalPayload digital;

        private PaymentStatusEnvelope(DigitalPayload digital) {
            this.digital = digital;
        }

        static PaymentStatusEnvelope digital(String id) {
            return new PaymentStatusEnvelope(new DigitalPayload(id));
        }

        public boolean hasDigital() {
            return digital != null;
        }

        public DigitalPayload getDigital() {
            return digital;
        }
    }

    abstract static class BaseStep implements Configurable {
        private StepConfig config = new StepConfig()
            .retryLimit(2)
            .retryWait(Duration.ofMillis(1))
            .maxBackoff(Duration.ofMillis(5))
            .backpressureBufferCapacity(32)
            .backpressureStrategy("BUFFER")
            .recoverOnFailure(false);

        @Override
        public StepConfig effectiveConfig() {
            return config;
        }

        @Override
        public void initialiseWithConfig(StepConfig config) {
            this.config = config;
        }

        protected void setRecoverOnFailure(boolean recoverOnFailure) {
            this.config = new StepConfig()
                .retryLimit(config.retryLimit())
                .retryWait(config.retryWait())
                .maxBackoff(config.maxBackoff())
                .backpressureBufferCapacity(config.backpressureBufferCapacity())
                .backpressureStrategy(config.backpressureStrategy())
                .recoverOnFailure(recoverOnFailure);
        }
    }

    static final class PrefixStep extends BaseStep implements StepOneToOne<Payload, Payload> {
        @Override
        public Uni<Payload> applyOneToOne(Payload in) {
            return Uni.createFrom().item(new Payload("pref-" + in.value()));
        }
    }

    static final class SplitStep extends BaseStep implements StepOneToMany<Payload, Payload> {
        @Override
        public Multi<Payload> applyOneToMany(Payload in) {
            return Multi.createFrom().items(
                new Payload(in.value() + "|pref-alpha-a"),
                new Payload(in.value() + "|pref-alpha-b"));
        }
    }

    static final class MergeStep extends BaseStep implements StepManyToOne<Payload, Payload> {
        @Override
        public Uni<Payload> applyReduce(Multi<Payload> input) {
            return input.collect().asList()
                .onItem().transform(items -> new Payload(items.get(0).value() + "+" + items.get(1).value()));
        }
    }

    static final class TransformStep extends BaseStep implements StepManyToMany<Payload, Payload> {
        @Override
        public Multi<Payload> applyTransform(Multi<Payload> input) {
            return input.onItem().transform(item -> new Payload("transformed-" + item.value()));
        }
    }

    static final class RetryOnceStep extends BaseStep implements StepOneToOne<Payload, Payload> {
        private final AtomicInteger attempts = new AtomicInteger();

        @Override
        public Uni<Payload> applyOneToOne(Payload in) {
            if (attempts.getAndIncrement() == 0) {
                return Uni.createFrom().failure(new IllegalStateException("retry me"));
            }
            return Uni.createFrom().item(new Payload("retry-" + in.value()));
        }
    }

    static final class AlwaysFailStep extends BaseStep implements StepOneToOne<Payload, Payload> {
        @Override
        public Uni<Payload> applyOneToOne(Payload in) {
            return Uni.createFrom().failure(new IllegalArgumentException("boom"));
        }
    }

    static final class NeverCompletesStep extends BaseStep implements StepOneToOne<Payload, Payload> {
        @Override
        public Uni<Payload> applyOneToOne(Payload in) {
            return Uni.createFrom().nothing();
        }
    }

    static final class RejectingStep extends BaseStep implements StepOneToOne<Payload, Payload> {
        RejectingStep() {
            setRecoverOnFailure(true);
        }

        @Override
        public Uni<Payload> applyOneToOne(Payload in) {
            return Uni.createFrom().failure(new IllegalStateException("reject this"));
        }
    }

    static final class PhysicalOnlyStep extends BaseStep implements StepOneToOne<PhysicalPayload, PhysicalHandled> {
        @Override
        public Uni<PhysicalHandled> applyOneToOne(PhysicalPayload in) {
            return Uni.createFrom().item(new PhysicalHandled(in.id()));
        }
    }

    static final class CollectingExporter implements PipelineReplayExporter {
        final List<PipelineExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());
        Long durationMs;
        String status;
        String failureType;
        String failureMessage;
        PipelineReplayRunParameters runParameters;

        @Override
        public void runStarted(
            String runId,
            String pipeline,
            Instant startedAt,
            PipelineReplayRunParameters runParameters,
            PipelineReplayTopology topology
        ) {
            this.runParameters = runParameters;
        }

        @Override
        public void emit(String runId, PipelineExecutionEvent event) {
            events.add(event);
        }

        @Override
        public void runCompleted(
            String runId,
            String pipeline,
            Instant startedAt,
            long durationMs,
            PipelineReplayTopology topology
        ) {
            this.durationMs = durationMs;
            this.status = "completed";
        }

        @Override
        public void runFailed(
            String runId,
            String pipeline,
            Instant startedAt,
            long durationMs,
            PipelineReplayTopology topology,
            Throwable failure) {
            this.durationMs = durationMs;
            this.status = "failed";
            this.failureType = failure == null ? null : failure.getClass().getName();
            this.failureMessage = failure == null ? null : failure.getMessage();
        }
    }

    static class ReplayEnabledPipelineStepConfig implements PipelineStepConfig {
        private final String replayFilePath;

        ReplayEnabledPipelineStepConfig() {
            this("/tmp/replay.json");
        }

        ReplayEnabledPipelineStepConfig(String replayFilePath) {
            this.replayFilePath = replayFilePath;
        }

        private final PipelineStepConfig.StepConfig defaults = new PipelineStepConfig.StepConfig() {
            @Override
            public Integer retryLimit() {
                return 2;
            }

            @Override
            public Long retryWaitMs() {
                return 1L;
            }

            @Override
            public Boolean recoverOnFailure() {
                return false;
            }

            @Override
            public Long maxBackoff() {
                return 5L;
            }

            @Override
            public Boolean jitter() {
                return false;
            }

            @Override
            public Integer backpressureBufferCapacity() {
                return 32;
            }

            @Override
            public String backpressureStrategy() {
                return "BUFFER";
            }
        };

        @Override
        public PipelineStepConfig.StepConfig defaults() {
            return defaults;
        }

        @Override
        public ParallelismPolicy parallelism() {
            return ParallelismPolicy.AUTO;
        }

        @Override
        public Integer maxConcurrency() {
            return 4;
        }

        @Override
        public AwaitAdmissionConfig awaitAdmission() {
            return new AwaitAdmissionConfig() {
                @Override
                public boolean enabled() {
                    return false;
                }

                @Override
                public String store() {
                    return "in-memory";
                }

                @Override
                public long retryWaitMs() {
                    return 100;
                }
            };
        }

        @Override
        public HealthConfig health() {
            return new HealthConfig() {
                @Override
                public Boolean enabled() {
                    return true;
                }

                @Override
                public Duration startupTimeout() {
                    return Duration.ofMinutes(5);
                }
            };
        }

        @Override
        public CacheConfig cache() {
            return new CacheConfig() {
                @Override
                public Optional<String> provider() {
                    return Optional.empty();
                }

                @Override
                public String policy() {
                    return "prefer-cache";
                }

                @Override
                public Optional<Duration> ttl() {
                    return Optional.empty();
                }

                @Override
                public CaffeineConfig caffeine() {
                    return null;
                }

                @Override
                public RedisConfig redis() {
                    return null;
                }
            };
        }

        @Override
        public TelemetryConfig telemetry() {
            return new TelemetryConfig() {
                @Override
                public Boolean enabled() {
                    return true;
                }

                @Override
                public Optional<String> itemInputType() {
                    return Optional.empty();
                }

                @Override
                public Optional<String> itemOutputType() {
                    return Optional.empty();
                }

                @Override
                public TracingConfig tracing() {
                    return new TracingConfig() {
                        @Override
                        public Boolean enabled() {
                            return true;
                        }

                        @Override
                        public Boolean perItem() {
                            return true;
                        }

                        @Override
                        public Boolean clientSpansForce() {
                            return false;
                        }

                        @Override
                        public Optional<String> clientSpansAllowlist() {
                            return Optional.empty();
                        }
                    };
                }

                @Override
                public MetricsConfig metrics() {
                    return () -> true;
                }

                @Override
                public ReplayConfig replay() {
                    return new ReplayConfig() {
                        @Override
                        public Boolean enabled() {
                            return true;
                        }

                        @Override
                        public String exporter() {
                            return "file";
                        }

                        @Override
                        public Optional<String> filePath() {
                            return Optional.of(replayFilePath);
                        }
                    };
                }
            };
        }

        @Override
        public KillSwitchConfig killSwitch() {
            return () -> new RetryAmplificationGuardConfig() {
                @Override
                public Boolean enabled() {
                    return false;
                }

                @Override
                public Duration window() {
                    return Duration.ofSeconds(30);
                }

                @Override
                public Double inflightSlopeThreshold() {
                    return 10d;
                }

                @Override
                public RetryAmplificationGuardMode mode() {
                    return RetryAmplificationGuardMode.FAIL_FAST;
                }

                @Override
                public Integer sustainSamples() {
                    return 3;
                }
            };
        }

        @Override
        public Map<String, PipelineStepConfig.StepConfig> step() {
            return Map.of();
        }

        @Override
        public Map<String, ModuleConfig> module() {
            return Map.of();
        }

        @Override
        public ClientConfig client() {
            return new ClientConfig() {
                @Override
                public Optional<Integer> basePort() {
                    return Optional.empty();
                }

                @Override
                public Optional<String> tlsConfigurationName() {
                    return Optional.empty();
                }
            };
        }
    }

    static final class ReplayDisabledPipelineStepConfig extends ReplayEnabledPipelineStepConfig {
        ReplayDisabledPipelineStepConfig(String replayFilePath) {
            super(replayFilePath);
        }

        @Override
        public TelemetryConfig telemetry() {
            TelemetryConfig delegate = super.telemetry();
            return new TelemetryConfig() {
                @Override
                public Boolean enabled() {
                    return delegate.enabled();
                }

                @Override
                public Optional<String> itemInputType() {
                    return delegate.itemInputType();
                }

                @Override
                public Optional<String> itemOutputType() {
                    return delegate.itemOutputType();
                }

                @Override
                public TracingConfig tracing() {
                    return delegate.tracing();
                }

                @Override
                public MetricsConfig metrics() {
                    return delegate.metrics();
                }

                @Override
                public ReplayConfig replay() {
                    return new ReplayConfig() {
                        @Override
                        public Boolean enabled() {
                            return false;
                        }

                        @Override
                        public String exporter() {
                            return "file";
                        }

                        @Override
                        public Optional<String> filePath() {
                            return delegate.replay().filePath();
                        }
                    };
                }
            };
        }
    }

    static final class ReplayMissingPrerequisitesPipelineStepConfig extends ReplayEnabledPipelineStepConfig {
        ReplayMissingPrerequisitesPipelineStepConfig(String replayFilePath) {
            super(replayFilePath);
        }

        @Override
        public TelemetryConfig telemetry() {
            TelemetryConfig delegate = super.telemetry();
            return new TelemetryConfig() {
                @Override
                public Boolean enabled() {
                    return delegate.enabled();
                }

                @Override
                public Optional<String> itemInputType() {
                    return delegate.itemInputType();
                }

                @Override
                public Optional<String> itemOutputType() {
                    return delegate.itemOutputType();
                }

                @Override
                public TracingConfig tracing() {
                    return new TracingConfig() {
                        @Override
                        public Boolean enabled() {
                            return true;
                        }

                        @Override
                        public Boolean perItem() {
                            return false;
                        }

                        @Override
                        public Boolean clientSpansForce() {
                            return false;
                        }

                        @Override
                        public Optional<String> clientSpansAllowlist() {
                            return Optional.empty();
                        }
                    };
                }

                @Override
                public MetricsConfig metrics() {
                    return delegate.metrics();
                }

                @Override
                public ReplayConfig replay() {
                    return delegate.replay();
                }
            };
        }
    }

    private static String findParameterValue(PipelineReplayRunParameters runParameters, String key) {
        return runParameters.sections().stream()
            .flatMap(section -> section.entries().stream())
            .filter(entry -> key.equals(entry.key()))
            .map(entry -> entry.value())
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing replay parameter: " + key));
    }

}
