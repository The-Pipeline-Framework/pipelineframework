package org.pipelineframework;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.PipelineTransitionWorkerSelector;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.objectpublish.ObjectPayloadChunk;
import org.pipelineframework.objectpublish.ObjectPublishGroupRenderer;
import org.pipelineframework.objectpublish.ObjectPublishRunner;
import org.pipelineframework.objectpublish.ObjectPublishTelemetry;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectTargetRegistry;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.objectpublish.StreamingObjectPublishMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineExecutionServiceTest {

    private PipelineExecutionService service;

    @Mock
    private PipelineControlPlane controlPlane;

    @Mock
    private PipelineTransitionWorkerSelector transitionWorkerSelector;

    @Mock
    private PipelineReleaseIdentityResolver releaseIdentityResolver;

    @Mock
    private PipelineRunner pipelineRunner;

    @Mock
    private PipelineStepResolver pipelineStepResolver;

    @Mock
    private AwaitCoordinator awaitCoordinator;

    @BeforeEach
    void setUp() {
        service = new PipelineExecutionService();
        service.controlPlane = controlPlane;
        service.transitionWorkerSelector = transitionWorkerSelector;
        service.releaseIdentityResolver = releaseIdentityResolver;
        service.pipelineRunner = pipelineRunner;
        service.pipelineStepResolver = pipelineStepResolver;
        service.awaitCoordinator = awaitCoordinator;
        service.executionHooks = new ExecutionHooks();
        service.executionInputPolicy = new ExecutionInputPolicy();
    }

    @Test
    void startupHealthStateInitiallyPending() {
        assertEquals(PipelineExecutionService.StartupHealthState.PENDING, service.getStartupHealthState());
    }

    @Test
    void executePipelineAsyncDefaultsToUnaryOutputMode() {
        RunAsyncAcceptedDto expected = new RunAsyncAcceptedDto("exec-1", false, "/pipeline/executions/exec-1", 1L);
        when(controlPlane.executePipelineAsync("input", "tenant-1", "idem-1", false))
            .thenReturn(Uni.createFrom().item(expected));

        RunAsyncAcceptedDto actual = service.executePipelineAsync("input", "tenant-1", "idem-1")
            .await().indefinitely();

        assertEquals(expected.executionId(), actual.executionId());
        verify(controlPlane).executePipelineAsync("input", "tenant-1", "idem-1", false);
    }

    @Test
    void executePipelineAsyncPassesOutputStreamingFlag() {
        RunAsyncAcceptedDto expected = new RunAsyncAcceptedDto("exec-2", false, "/pipeline/executions/exec-2", 2L);
        when(controlPlane.executePipelineAsync("input", "tenant-1", "idem-1", true))
            .thenReturn(Uni.createFrom().item(expected));

        RunAsyncAcceptedDto actual = service.executePipelineAsync("input", "tenant-1", "idem-1", true)
            .await().indefinitely();

        assertEquals(expected.executionId(), actual.executionId());
        verify(controlPlane).executePipelineAsync("input", "tenant-1", "idem-1", true);
    }

    @Test
    void getExecutionStatusDelegatesToControlPlane() {
        ExecutionStatusDto expected = new ExecutionStatusDto("exec-3", null, 0, 0, 1L, 0L, 0L, null, null);
        when(controlPlane.getExecutionStatus("tenant-1", "exec-3"))
            .thenReturn(Uni.createFrom().item(expected));

        ExecutionStatusDto actual = service.getExecutionStatus("tenant-1", "exec-3").await().indefinitely();

        assertEquals("exec-3", actual.executionId());
        verify(controlPlane).getExecutionStatus("tenant-1", "exec-3");
    }

    @Test
    void processExecutionWorkItemDelegatesToControlPlane() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant-1", "exec-4");
        PipelineTransitionWorker selectedWorker = service;
        when(transitionWorkerSelector.select(service)).thenReturn(selectedWorker);
        when(controlPlane.processExecutionWorkItem(
                eq(item),
                eq(selectedWorker),
                org.mockito.ArgumentMatchers.any()))
            .thenReturn(Uni.createFrom().voidItem());

        service.processExecutionWorkItem(item).await().indefinitely();

        verify(controlPlane).processExecutionWorkItem(
            eq(item),
            eq(selectedWorker),
            org.mockito.ArgumentMatchers.any());
    }

    @Test
    void queryPendingAwaitInteractionsNormalizesBlankFilters() {
        when(controlPlane.queryPendingAwaitInteractions(
                eq("tenant-1"),
                isNull(),
                isNull(),
                isNull(),
                eq(42)))
            .thenReturn(Uni.createFrom().item(List.of()));

        service.queryPendingAwaitInteractions(
            "tenant-1",
            " ",
            "",
            "\t",
            42).await().indefinitely();

        verify(controlPlane).queryPendingAwaitInteractions(
            eq("tenant-1"),
            isNull(),
            isNull(),
            isNull(),
            eq(42));
    }

    @Test
    void executeTransitionRejectsMismatchedReleaseIdentityBeforePayloadDecode() {
        TransitionCommandEnvelope command = new TransitionCommandEnvelope(
            "tenant-1",
            "exec-5",
            "other-pipeline",
            "sha256:other",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-5:0:0",
            "trace-5",
            "java.io.File",
            "application/tpf-transition+json",
            "{not-json");
        when(releaseIdentityResolver.validateCommandIdentity(command, null))
            .thenReturn(Optional.of("identity mismatch sentinel"));

        TransitionResultEnvelope result = service.executeTransition(command).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.FAILED, result.outcome());
        assertTrue(result.failure().message().contains("identity mismatch sentinel"));
    }

    @Test
    void executeTransitionAllowsLocalFallbackIdentityForInProcessWorker() throws Exception {
        markStartupHealthy(service);
        TransitionCommandEnvelope command = new TransitionCommandEnvelope(
            "tenant-1",
            "exec-6",
            "local-pipeline",
            "local-contract",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-6:0:0",
            "trace-6",
            "java.lang.String",
            "application/tpf-transition+json",
            "\"input\"");
        TransitionResultEnvelope result = service.executeTransition(command).await().indefinitely();

        assertFalse(
            result.outcome() == TransitionWorkerOutcome.FAILED
                && result.failure().message().contains("identity mismatch sentinel"));
        verify(releaseIdentityResolver, never()).validateCommandIdentity(command, null);
    }

    @Test
    void executePortableTransitionRejectsLocalFallbackIdentity() {
        TransitionCommandEnvelope command = new TransitionCommandEnvelope(
            "tenant-1",
            "exec-7",
            "local-pipeline",
            "local-contract",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-7:0:0",
            "trace-7",
            "java.lang.String",
            "application/tpf-transition+json",
            "\"input\"");
        when(releaseIdentityResolver.validateCommandIdentity(command, null))
            .thenReturn(Optional.of("identity mismatch sentinel"));

        TransitionResultEnvelope result = service.executePortableTransition(command).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.FAILED, result.outcome());
        assertTrue(result.failure().message().contains("identity mismatch sentinel"));
    }

    @Test
    void executeTransitionWaitsForStreamingObjectPublishBeforeReturningCompletedEnvelope() throws Exception {
        markStartupHealthy(service);
        JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
        service.transitionPayloadCodec = codec;
        List<Object> steps = List.of(new Object());
        when(pipelineStepResolver.loadPipelineSteps()).thenReturn(steps);
        GatedObjectTargetProvider provider = new GatedObjectTargetProvider();
        ObjectPublishRunner publishRunner = new ObjectPublishRunner(
            objectPublishConfig("gated"),
            new ObjectTargetRegistry(List.of(provider)),
            ObjectPublishTelemetry.NOOP);
        @SuppressWarnings("unchecked")
        Multi<TestTerminalOutput> published = (Multi<TestTerminalOutput>) publishRunner.publish(
            Multi.createFrom().item(new TestTerminalOutput("file-a", "line-1")));
        when(pipelineRunner.runFromStepUntilWithContext(any(), eq(steps), eq(0), eq(1)))
            .thenReturn(new PipelineRunner.ExecutionResult(published, null));

        CompletableFuture<TransitionResultEnvelope> result = service.executeTransition(
            transitionCommand(codec, 0, -1)).subscribeAsCompletionStage();

        provider.awaitWriteStarted();
        assertFalse(result.isDone(), "Transition result must wait for the streaming object write to be accepted");
        assertEquals(1, provider.writeAttempts());
        provider.acceptWrite();

        TransitionResultEnvelope envelope = result.get(5, TimeUnit.SECONDS);

        assertEquals(TransitionWorkerOutcome.COMPLETED, envelope.outcome());
        assertEquals(List.of(new TestTerminalOutput("file-a", "line-1")), envelope.decodeOutputItems(codec));
        assertEquals("line-1\n", provider.body());
        verify(pipelineRunner).runFromStepUntilWithContext(any(), eq(steps), eq(0), eq(1));
    }

    @Test
    void executeTransitionReturnsFailedEnvelopeWhenStreamingObjectPublishFails() throws Exception {
        markStartupHealthy(service);
        JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
        service.transitionPayloadCodec = codec;
        List<Object> steps = List.of(new Object());
        when(pipelineStepResolver.loadPipelineSteps()).thenReturn(steps);
        ObjectPublishRunner publishRunner = new ObjectPublishRunner(
            objectPublishConfig("failing"),
            new ObjectTargetRegistry(List.of(new FailingObjectTargetProvider())),
            ObjectPublishTelemetry.NOOP);
        @SuppressWarnings("unchecked")
        Multi<TestTerminalOutput> published = (Multi<TestTerminalOutput>) publishRunner.publish(
            Multi.createFrom().item(new TestTerminalOutput("file-a", "line-1")));
        when(pipelineRunner.runFromStepUntilWithContext(any(), eq(steps), eq(0), eq(1)))
            .thenReturn(new PipelineRunner.ExecutionResult(published, null));

        TransitionResultEnvelope envelope = service.executeTransition(transitionCommand(codec, 0, -1))
            .await().indefinitely();

        assertEquals(TransitionWorkerOutcome.FAILED, envelope.outcome());
        assertTrue(envelope.failure().message().contains("publish failed"));
    }

    @Test
    void executeTransitionReturnsWaitingEnvelopeWhenItemizedAwaitSuspendsAfterSingleSourceSubscription() throws Exception {
        markStartupHealthy(service);
        JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
        service.transitionPayloadCodec = codec;
        List<Object> steps = List.of(new Object());
        AtomicInteger subscriptions = new AtomicInteger();
        Multi<Object> parentStream = Multi.createFrom().deferred(() -> {
            subscriptions.incrementAndGet();
            return Multi.createFrom().failure(new AwaitSuspendedException(
                "tenant-1",
                "exec-stream",
                "unit-1",
                2));
        });
        when(pipelineStepResolver.loadPipelineSteps()).thenReturn(steps);
        when(pipelineRunner.runFromStepUntilWithContext(any(), eq(steps), eq(0), eq(1)))
            .thenReturn(new PipelineRunner.ExecutionResult(parentStream, null));
        when(awaitCoordinator.suspensionSnapshot(any(AwaitSuspendedException.class)))
            .thenReturn(Uni.createFrom().item(new org.pipelineframework.orchestrator.TransitionAwaitSuspension(
                "tenant-1",
                "exec-stream",
                "unit-1",
                2)));

        TransitionResultEnvelope envelope = service.executeTransition(transitionCommand(codec, 0, -1))
            .await().indefinitely();

        assertEquals(TransitionWorkerOutcome.WAITING_EXTERNAL, envelope.outcome());
        assertEquals("unit-1", envelope.awaitSuspension().unitId());
        assertEquals(1, subscriptions.get());
    }

    @Test
    void executeTransitionDoesNotRunPipelineOrPublishWhenStopIndexEqualsCurrentIndex() throws Exception {
        markStartupHealthy(service);
        JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
        service.transitionPayloadCodec = codec;
        when(pipelineStepResolver.loadPipelineSteps()).thenReturn(List.of(new Object()));

        TransitionResultEnvelope envelope = service.executeTransition(transitionCommand(codec, 0, 0))
            .await().indefinitely();

        assertEquals(TransitionWorkerOutcome.COMPLETED, envelope.outcome());
        assertEquals(List.of("input"), envelope.decodeOutputItems(codec));
        verify(pipelineRunner, never()).runFromStepUntilWithContext(any(), any(), any(Integer.class), any(Integer.class));
    }

    private static TransitionCommandEnvelope transitionCommand(
        JsonTransitionPayloadCodec codec,
        int currentStepIndex,
        int stopBeforeStepIndex
    ) {
        var payload = codec.encode("input");
        return new TransitionCommandEnvelope(
            "tenant-1",
            "exec-stream",
            PipelineContractDescriptor.DEFAULT_PIPELINE_ID,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            currentStepIndex,
            stopBeforeStepIndex,
            0,
            ExecutionResultShape.MATERIALIZED_MULTI,
            0L,
            "transition-1",
            "trace-1",
            payload.payloadTypeId(),
            payload.payloadEncoding(),
            payload.payload());
    }

    private static PipelineYamlConfig objectPublishConfig(String provider) {
        return new PipelineYamlConfig(
            "org.pipelineframework",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of(),
            Map.of(),
            Map.of("results", new PipelineObjectPublishConfig(
                "results",
                "object",
                provider,
                Map.of(),
                new PipelineObjectNamingConfig("{groupKey}.out"),
                null,
                new PipelineObjectPublishGroupingConfig(32))),
            List.of(),
            null,
            new PipelineOutputBoundaryConfig(null, new PipelineObjectOutputConfig(
                "results",
                TestTerminalOutput.class.getName(),
                "TestTerminalOutput",
                StreamingTransitionMapper.class.getName())));
    }

    @SuppressWarnings("unchecked")
    private static void markStartupHealthy(PipelineExecutionService service) throws Exception {
        Field field = PipelineExecutionService.class.getDeclaredField("startupHealthState");
        field.setAccessible(true);
        AtomicReference<PipelineExecutionService.StartupHealthState> state =
            (AtomicReference<PipelineExecutionService.StartupHealthState>) field.get(service);
        state.set(PipelineExecutionService.StartupHealthState.HEALTHY);
    }

    public record TestTerminalOutput(String group, String value) {
    }

    public static final class StreamingTransitionMapper implements StreamingObjectPublishMapper<TestTerminalOutput> {
        @Override
        public String groupKey(TestTerminalOutput item) {
            return item.group();
        }

        @Override
        public ObjectPublishGroupRenderer<TestTerminalOutput> openGroup(String groupKey, TestTerminalOutput firstItem) {
            return new ObjectPublishGroupRenderer<>() {
                @Override
                public String contentType() {
                    return "text/plain";
                }

                @Override
                public ObjectPayloadChunk onItem(TestTerminalOutput item) {
                    return new ObjectPayloadChunk((item.value() + "\n").getBytes(
                        java.nio.charset.StandardCharsets.UTF_8));
                }
            };
        }
    }

    private static final class GatedObjectTargetProvider implements ObjectTargetProvider {
        private final CompletableFuture<Void> writeStarted = new CompletableFuture<>();
        private final CompletableFuture<Void> writeAccepted = new CompletableFuture<>();
        private final AtomicInteger writeAttempts = new AtomicInteger();
        private final AtomicReference<String> body = new AtomicReference<>("");

        @Override
        public String providerName() {
            return "gated";
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.completedFuture(new ObjectWriteSession() {
                @Override
                public CompletionStage<Void> write(ByteBuffer chunk) {
                    ByteBuffer duplicate = chunk.slice();
                    byte[] bytes = new byte[duplicate.remaining()];
                    duplicate.get(bytes);
                    body.updateAndGet(current ->
                        current + new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
                    writeAttempts.incrementAndGet();
                    writeStarted.complete(null);
                    return writeAccepted;
                }

                @Override
                public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
                    return CompletableFuture.completedFuture(new ObjectWriteResult(
                        null,
                        closeRequest.bytes(),
                        closeRequest.checksum(),
                        null));
                }

                @Override
                public CompletionStage<Void> abort(Throwable cause) {
                    writeAccepted.complete(null);
                    return CompletableFuture.completedFuture(null);
                }
            });
        }

        private void awaitWriteStarted() throws Exception {
            writeStarted.get(5, TimeUnit.SECONDS);
        }

        private void acceptWrite() {
            writeAccepted.complete(null);
        }

        private int writeAttempts() {
            return writeAttempts.get();
        }

        private String body() {
            return body.get();
        }
    }

    private static final class FailingObjectTargetProvider implements ObjectTargetProvider {
        @Override
        public String providerName() {
            return "failing";
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.completedFuture(new ObjectWriteSession() {
                @Override
                public CompletionStage<Void> write(ByteBuffer chunk) {
                    return CompletableFuture.failedFuture(new IllegalStateException("publish failed"));
                }

                @Override
                public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
                    return CompletableFuture.completedFuture(new ObjectWriteResult(null, 0, "", null));
                }

                @Override
                public CompletionStage<Void> abort(Throwable cause) {
                    return CompletableFuture.completedFuture(null);
                }
            });
        }
    }
}
