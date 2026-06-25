package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ControlPlaneTransitionAdmission;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutionMode;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;

@ExtendWith(MockitoExtension.class)
class QueueAsyncExecutionFlowTest {

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    private PipelineOrchestratorConfig.WorkerConfig workerConfig;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private QueueAsyncExecutionFlow.Effects effects;

    @Mock
    private PipelineTransitionWorker worker;

    @Mock
    private AwaitItemContinuationHandler itemContinuationHandler;

    private TransitionWorkerExecutor transitionWorkerExecutor;
    private QueueAsyncExecutionFlow flow;

    @BeforeEach
    void setUp() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        lenient().when(orchestratorConfig.leaseMs()).thenReturn(1_000L);
        when(orchestratorConfig.worker()).thenReturn(workerConfig);
        when(workerConfig.maxInFlight()).thenReturn(4);
        lenient().when(workerConfig.executionMode()).thenReturn(TransitionWorkerExecutionMode.SAME_THREAD);
        transitionWorkerExecutor = new TransitionWorkerExecutor(orchestratorConfig, new PipelineInvocationRuntime());
        flow = new QueueAsyncExecutionFlow(
            orchestratorConfig,
            executionStateStore,
            workDispatcher,
            transitionWorkerExecutor,
            "worker-1",
            effects);
    }

    @Test
    void processWorkItemFlowsThroughClaimWorkerAndCommit() {
        ExecutionWorkItem workItem = new ExecutionWorkItem("tenant-1", "exec-1");
        ExecutionRecord<Object, Object> record = execution();
        TransitionCommandEnvelope command = command();
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"));
        AtomicInteger tenantPermitCloses = new AtomicInteger();
        when(effects.admitTransition(workItem))
            .thenReturn(ControlPlaneTransitionAdmission.admitted(tenantPermitCloses::incrementAndGet));
        when(executionStateStore.claimLease(
            eq("tenant-1"),
            eq("exec-1"),
            eq("worker-1"),
            anyLong(),
            eq(1_000L))).thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(effects.prepareTransitionCommand(record, "exec-1:3:0")).thenReturn(Uni.createFrom().item(command));
        when(worker.executeTransition(command)).thenReturn(Uni.createFrom().item(result));
        when(effects.commitTransition(record, "exec-1:3:0", result, itemContinuationHandler))
            .thenReturn(Uni.createFrom().voidItem());

        flow.processExecutionWorkItem(workItem, worker, itemContinuationHandler).await().indefinitely();

        InOrder inOrder = inOrder(effects, executionStateStore, worker);
        inOrder.verify(effects).admitTransition(workItem);
        inOrder.verify(executionStateStore).claimLease(
            eq("tenant-1"),
            eq("exec-1"),
            eq("worker-1"),
            anyLong(),
            eq(1_000L));
        inOrder.verify(effects).recordClaimed(record);
        inOrder.verify(effects).prepareTransitionCommand(record, "exec-1:3:0");
        inOrder.verify(worker).executeTransition(command);
        inOrder.verify(effects).commitTransition(record, "exec-1:3:0", result, itemContinuationHandler);
        assertEquals(1, tenantPermitCloses.get());
        assertEquals(0, transitionWorkerExecutor.activePermits());
    }

    @Test
    void saturatedTenantAdmissionRequeuesWithoutClaim() {
        ExecutionWorkItem workItem = new ExecutionWorkItem("tenant-1", "exec-1");
        Duration delay = Duration.ofMillis(250L);
        when(effects.admitTransition(workItem)).thenReturn(ControlPlaneTransitionAdmission.denied(
            ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED,
                "tenant quota saturated")));
        when(effects.saturatedDelay()).thenReturn(delay);
        when(workDispatcher.enqueueDelayed(workItem, delay)).thenReturn(Uni.createFrom().voidItem());

        flow.processExecutionWorkItem(workItem, worker, itemContinuationHandler).await().indefinitely();

        verify(workDispatcher).enqueueDelayed(workItem, delay);
        verify(executionStateStore, never()).claimLease(
            eq("tenant-1"),
            eq("exec-1"),
            eq("worker-1"),
            anyLong(),
            eq(1_000L));
        assertEquals(0, transitionWorkerExecutor.activePermits());
    }

    @Test
    void workerFailureRoutesThroughFailureEffect() {
        ExecutionWorkItem workItem = new ExecutionWorkItem("tenant-1", "exec-1");
        ExecutionRecord<Object, Object> record = execution();
        TransitionCommandEnvelope command = command();
        IllegalStateException failure = new IllegalStateException("worker failed");
        when(effects.admitTransition(workItem))
            .thenReturn(ControlPlaneTransitionAdmission.admitted(() -> {
            }));
        when(executionStateStore.claimLease(
            eq("tenant-1"),
            eq("exec-1"),
            eq("worker-1"),
            anyLong(),
            eq(1_000L))).thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(effects.prepareTransitionCommand(record, "exec-1:3:0")).thenReturn(Uni.createFrom().item(command));
        when(worker.executeTransition(command)).thenReturn(Uni.createFrom().failure(failure));
        when(effects.handleExecutionFailure(record, "exec-1:3:0", failure)).thenReturn(Uni.createFrom().voidItem());

        flow.processExecutionWorkItem(workItem, worker, itemContinuationHandler).await().indefinitely();

        verify(effects).handleExecutionFailure(record, "exec-1:3:0", failure);
        verify(effects, never()).commitTransition(any(), any(), any(), any());
    }

    @Test
    void awaitSuspensionRoutesThroughWaitingExternalEffect() {
        ExecutionWorkItem workItem = new ExecutionWorkItem("tenant-1", "exec-1");
        ExecutionRecord<Object, Object> record = execution();
        TransitionCommandEnvelope command = command();
        AwaitSuspendedException failure = new AwaitSuspendedException("tenant-1", "exec-1", "unit-1", 3);
        TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-1", "unit-1", 3);
        when(effects.admitTransition(workItem))
            .thenReturn(ControlPlaneTransitionAdmission.admitted(() -> {
            }));
        when(executionStateStore.claimLease(
            eq("tenant-1"),
            eq("exec-1"),
            eq("worker-1"),
            anyLong(),
            eq(1_000L))).thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(effects.prepareTransitionCommand(record, "exec-1:3:0")).thenReturn(Uni.createFrom().item(command));
        when(worker.executeTransition(command)).thenReturn(Uni.createFrom().failure(failure));
        when(effects.suspensionSnapshot(failure)).thenReturn(Uni.createFrom().item(suspension));
        when(effects.markWaitingExternal(record, "exec-1:3:0", suspension, itemContinuationHandler))
            .thenReturn(Uni.createFrom().voidItem());

        flow.processExecutionWorkItem(workItem, worker, itemContinuationHandler).await().indefinitely();

        verify(effects).suspensionSnapshot(failure);
        verify(effects).markWaitingExternal(record, "exec-1:3:0", suspension, itemContinuationHandler);
        verify(effects, never()).handleExecutionFailure(eq(record), eq("exec-1:3:0"), eq(failure));
    }

    private static TransitionCommandEnvelope command() {
        return new TransitionCommandEnvelope(
            "tenant-1",
            "exec-1",
            "pipeline",
            "release",
            3,
            0,
            ExecutionResultShape.MATERIALIZED_MULTI,
            1L,
            "exec-1:3:0",
            "trace-1",
            String.class.getName(),
            "plain",
            "input");
    }

    private static ExecutionRecord<Object, Object> execution() {
        return new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "exec-key",
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.MATERIALIZED_MULTI,
            ExecutionStatus.RUNNING,
            1L,
            3,
            0,
            "worker",
            0L,
            0L,
            "previous",
            new SerializedTransitionPayload(String.class.getName(), "plain", "input"),
            null,
            null,
            null,
            null,
            10_000L,
            10_000L,
            86_400L);
    }
}
