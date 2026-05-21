package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwaitStepSupportTest {

    @Mock
    PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    AwaitCoordinator awaitCoordinator;

    @AfterEach
    void clearContext() {
        AwaitExecutionContextHolder.clear();
    }

    @Test
    void awaitOneToOneFailsOutsideQueueAsyncMode() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.SYNC);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> support.awaitOneToOne(descriptor(), "input").await().indefinitely());

        assertTrue(error.getMessage().contains("pipeline.orchestrator.mode=QUEUE_ASYNC"));
    }

    @Test
    void awaitOneToOneFailsWithoutExecutionContext() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> support.awaitOneToOne(descriptor(), "input").await().indefinitely());

        assertTrue(error.getMessage().contains("without queue-async execution context"));
    }

    @Test
    void awaitOneToOneDelegatesInQueueAsyncMode() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec123", 0);
        AwaitExecutionContextHolder.set(context);

        AwaitStepDescriptor testDescriptor = descriptor();
        AwaitInteractionRecord mockRecord = new AwaitInteractionRecord(
            "tenant1", "exec123", "review", 0, String.class.getName(),
            "interaction-id", "correlation-id", "causation-id", "idem-key",
            0L, org.pipelineframework.awaitable.AwaitInteractionStatus.WAITING,
            "input", "output", null, null, null, "interaction-api",
            Map.of(), System.currentTimeMillis() + 300000, System.currentTimeMillis(),
            System.currentTimeMillis(), System.currentTimeMillis() + 86400);
        AwaitCreateResult mockCreateResult = new AwaitCreateResult(mockRecord, false);

        when(awaitCoordinator.createOrGet(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(0),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq("input"),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull()))
            .thenReturn(io.smallrye.mutiny.Uni.createFrom().item(mockCreateResult));
        when(awaitCoordinator.dispatch(testDescriptor, mockRecord))
            .thenReturn(io.smallrye.mutiny.Uni.createFrom().item(mockRecord));

        assertThrows(AwaitSuspendedException.class,
            () -> support.awaitOneToOne(testDescriptor, "input").await().indefinitely());

        org.mockito.Mockito.verify(awaitCoordinator).createOrGet(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(0),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq("input"),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull());
        org.mockito.Mockito.verify(awaitCoordinator).dispatch(testDescriptor, mockRecord);
    }

    @Test
    void awaitOneToOneWithDescriptorUniCapturesExecutionContextBeforeReactiveResolution() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant1", "exec123", 4));

        AwaitStepDescriptor testDescriptor = descriptor();
        AwaitInteractionRecord mockRecord = new AwaitInteractionRecord(
            "tenant1", "exec123", "review", 4, String.class.getName(),
            "interaction-id", "correlation-id", "causation-id", "idem-key",
            0L, org.pipelineframework.awaitable.AwaitInteractionStatus.WAITING,
            "input", "output", null, null, null, "interaction-api",
            Map.of(), System.currentTimeMillis() + 300000, System.currentTimeMillis(),
            System.currentTimeMillis(), System.currentTimeMillis() + 86400);
        AwaitCreateResult mockCreateResult = new AwaitCreateResult(mockRecord, false);

        Uni<String> await = support.awaitOneToOne(Uni.createFrom().item(testDescriptor), "input");
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant1", "exec123", 99));
        AwaitExecutionContextHolder.clear();

        when(awaitCoordinator.createOrGet(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(4),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq("input"),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull()))
            .thenReturn(Uni.createFrom().item(mockCreateResult));
        when(awaitCoordinator.dispatch(testDescriptor, mockRecord))
            .thenReturn(Uni.createFrom().item(mockRecord));

        assertThrows(AwaitSuspendedException.class, () -> await.await().indefinitely());

        org.mockito.Mockito.verify(awaitCoordinator).createOrGet(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(4),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq("input"),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull());
    }

    private AwaitStepSupport support() {
        AwaitStepSupport support = new AwaitStepSupport();
        support.orchestratorConfig = orchestratorConfig;
        support.awaitCoordinator = awaitCoordinator;
        return support;
    }

    private AwaitStepDescriptor descriptor() {
        return new AwaitStepDescriptor(
            "review",
            String.class.getName(),
            String.class.getName(),
            Duration.ofMinutes(5),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of());
    }
}
