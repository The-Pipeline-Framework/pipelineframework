package org.pipelineframework;

import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.PipelineBundleIdentityResolver;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.PipelineTransitionWorkerSelector;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
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
    private PipelineBundleIdentityResolver bundleIdentityResolver;

    @BeforeEach
    void setUp() {
        service = new PipelineExecutionService();
        service.controlPlane = controlPlane;
        service.transitionWorkerSelector = transitionWorkerSelector;
        service.bundleIdentityResolver = bundleIdentityResolver;
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
    void executeTransitionRejectsMismatchedBundleIdentityBeforePayloadDecode() {
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
        when(bundleIdentityResolver.validateCommandIdentity(command, null))
            .thenReturn(Optional.of("bundle mismatch"));

        TransitionResultEnvelope result = service.executeTransition(command).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.FAILED, result.outcome());
        assertTrue(result.failure().message().contains("bundle mismatch"));
    }

    @Test
    void executeTransitionAllowsLocalFallbackIdentityForInProcessWorker() {
        TransitionCommandEnvelope command = new TransitionCommandEnvelope(
            "tenant-1",
            "exec-6",
            "local-pipeline",
            "local-bundle",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-6:0:0",
            "trace-6",
            "java.lang.String",
            "application/tpf-transition+json",
            "\"input\"");
        when(bundleIdentityResolver.validateCommandIdentity(command, null))
            .thenReturn(Optional.of("bundle mismatch"));

        TransitionResultEnvelope result = service.executeTransition(command).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.FAILED, result.outcome());
        assertFalse(result.failure().message().contains("bundle mismatch"));
    }

    @Test
    void executePortableTransitionRejectsLocalFallbackIdentity() {
        TransitionCommandEnvelope command = new TransitionCommandEnvelope(
            "tenant-1",
            "exec-7",
            "local-pipeline",
            "local-bundle",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-7:0:0",
            "trace-7",
            "java.lang.String",
            "application/tpf-transition+json",
            "\"input\"");
        when(bundleIdentityResolver.validateCommandIdentity(command, null))
            .thenReturn(Optional.of("bundle mismatch"));

        TransitionResultEnvelope result = service.executePortableTransition(command).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.FAILED, result.outcome());
        assertTrue(result.failure().message().contains("bundle mismatch"));
    }
}
