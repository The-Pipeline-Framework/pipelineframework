package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
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

    @Test
    void awaitOneToOneStreamReinstallsExecutionContextInsidePerItemCallbacks() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant1", "exec123", 2));

        AwaitStepDescriptor testDescriptor = descriptor();
        when(awaitCoordinator.createOrGetItem(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(2),
            any(),
            any(),
            any(),
            anyInt(),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull()))
            .thenAnswer(invocation -> {
                if (AwaitExecutionContextHolder.get() == null) {
                    return Uni.createFrom().failure(new IllegalStateException("missing await execution context"));
                }
                Integer index = invocation.getArgument(7, Integer.class);
                String interactionId = "interaction-" + index;
                AwaitInteractionRecord record = new AwaitInteractionRecord(
                    "tenant1", "exec123", "review", 2, String.class.getName(),
                    interactionId, "correlation-" + index, "causation-" + index, "idem-" + index,
                    0L, org.pipelineframework.awaitable.AwaitInteractionStatus.WAITING,
                    invocation.getArgument(5), null, invocation.getArgument(6), index, null,
                    null, null,
                    "interaction-api", Map.of(), System.currentTimeMillis() + 300000, System.currentTimeMillis(),
                    System.currentTimeMillis(), System.currentTimeMillis() + 86400);
                return Uni.createFrom().item(new AwaitCreateResult(record, false));
            });
        when(awaitCoordinator.dispatch(org.mockito.ArgumentMatchers.eq(testDescriptor), any()))
            .thenAnswer(invocation -> Uni.createFrom().item(invocation.getArgument(1, AwaitInteractionRecord.class)));
        when(awaitCoordinator.markDispatchComplete(
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.anyString(),
            anyInt(),
            anyLong()))
            .thenAnswer(invocation -> Uni.createFrom().item(new AwaitUnitRecord(
                "tenant1",
                invocation.getArgument(1, String.class),
                "exec123",
                testDescriptor.stepId(),
                2,
                testDescriptor.cardinality(),
                0L,
                AwaitUnitStatus.WAITING_EXTERNAL,
                null,
                null,
                0,
                java.util.Set.of(),
                true,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                System.currentTimeMillis() + 86400)));

        assertThrows(
            AwaitSuspendedException.class,
            () -> support.awaitOneToOneStream(testDescriptor, Multi.createFrom().items("first", "second"))
                .collect().asList()
                .await().indefinitely());
    }

    @Test
    void awaitOneToOneStreamSubscribesColdSourceOnceWhenSuspendingAfterDispatch() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant1", "exec123", 2));

        AwaitStepDescriptor testDescriptor = descriptor();
        AtomicInteger subscriptions = new AtomicInteger();
        Multi<String> coldSource = Multi.createFrom().deferred(() -> {
            subscriptions.incrementAndGet();
            return Multi.createFrom().items("first", "second", "third");
        });
        when(awaitCoordinator.createOrGetItem(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(2),
            any(),
            any(),
            any(),
            anyInt(),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull()))
            .thenAnswer(invocation -> {
                Integer index = invocation.getArgument(7, Integer.class);
                AwaitInteractionRecord record = new AwaitInteractionRecord(
                    "tenant1", "exec123", "review", 2, String.class.getName(),
                    "interaction-" + index, "correlation-" + index, "causation-" + index, "idem-" + index,
                    0L, AwaitInteractionStatus.WAITING,
                    invocation.getArgument(5), null, invocation.getArgument(6), index, null,
                    null, null,
                    "interaction-api", Map.of(), System.currentTimeMillis() + 300000, System.currentTimeMillis(),
                    System.currentTimeMillis(), System.currentTimeMillis() + 86400);
                return Uni.createFrom().item(new AwaitCreateResult(record, false));
            });
        when(awaitCoordinator.dispatch(org.mockito.ArgumentMatchers.eq(testDescriptor), any()))
            .thenAnswer(invocation -> Uni.createFrom().item(invocation.getArgument(1, AwaitInteractionRecord.class)));
        when(awaitCoordinator.markDispatchComplete(
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(3),
            anyLong()))
            .thenAnswer(invocation -> Uni.createFrom().item(new AwaitUnitRecord(
                "tenant1",
                invocation.getArgument(1, String.class),
                "exec123",
                testDescriptor.stepId(),
                2,
                testDescriptor.cardinality(),
                1L,
                AwaitUnitStatus.WAITING_EXTERNAL,
                null,
                3,
                0,
                java.util.Set.of(),
                true,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                System.currentTimeMillis() + 86400)));

        assertThrows(
            AwaitSuspendedException.class,
            () -> support.<String, String>awaitOneToOneStream(testDescriptor, coldSource)
                .collect().asList()
                .await().indefinitely());

        assertEquals(1, subscriptions.get());
    }

    @Test
    void awaitOneToOneStreamContinuesWhenUnitCompletesBeforeSuspensionIsPersisted() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant1", "exec123", 2));

        AwaitStepDescriptor testDescriptor = descriptor();
        when(awaitCoordinator.createOrGetItem(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(2),
            any(),
            any(),
            any(),
            anyInt(),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull()))
            .thenAnswer(invocation -> {
                Integer index = invocation.getArgument(7, Integer.class);
                AwaitInteractionRecord record = new AwaitInteractionRecord(
                    "tenant1", "exec123", "review", 2, String.class.getName(),
                    "interaction-" + index, "correlation-" + index, "causation-" + index, "idem-" + index,
                    0L, AwaitInteractionStatus.WAITING,
                    invocation.getArgument(5), null, invocation.getArgument(6), index, null,
                    null, null,
                    "interaction-api", Map.of(), System.currentTimeMillis() + 300000, System.currentTimeMillis(),
                    System.currentTimeMillis(), System.currentTimeMillis() + 86400);
                return Uni.createFrom().item(new AwaitCreateResult(record, false));
            });
        when(awaitCoordinator.dispatch(org.mockito.ArgumentMatchers.eq(testDescriptor), any()))
            .thenAnswer(invocation -> Uni.createFrom().item(invocation.getArgument(1, AwaitInteractionRecord.class)));
        when(awaitCoordinator.markDispatchComplete(
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.anyString(),
            anyInt(),
            anyLong()))
            .thenAnswer(invocation -> Uni.createFrom().item(new AwaitUnitRecord(
                "tenant1",
                invocation.getArgument(1, String.class),
                "exec123",
                testDescriptor.stepId(),
                2,
                testDescriptor.cardinality(),
                1L,
                AwaitUnitStatus.COMPLETED,
                null,
                2,
                2,
                java.util.Set.of("item:0", "item:1"),
                true,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                System.currentTimeMillis() + 86400)));
        when(awaitCoordinator.loadResumePayload(
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.anyString()))
            .thenReturn(Uni.createFrom().item(List.of("approved-first", "approved-second")));

        List<String> output = support.<String, String>awaitOneToOneStream(
                testDescriptor,
                Multi.createFrom().items("first", "second"))
            .collect().asList()
            .await().indefinitely();

        assertEquals(List.of("approved-first", "approved-second"), output);
    }

    @Test
    void awaitManyToManyMaterializesInputIntoSingleAggregateInteraction() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant1", "exec123", 5));

        AwaitStepDescriptor testDescriptor = new AwaitStepDescriptor(
            "batch-review",
            String.class.getName(),
            String.class.getName(),
            "MANY_TO_MANY",
            Duration.ofMinutes(5),
            "signedResumeToken",
            "kafka",
            Map.of(),
            List.of());
        AwaitInteractionRecord mockRecord = new AwaitInteractionRecord(
            "tenant1", "exec123", "batch-review", 5, String.class.getName(),
            "interaction-id", "correlation-id", "causation-id", "idem-key",
            0L, org.pipelineframework.awaitable.AwaitInteractionStatus.WAITING,
            List.of("first", "second"), null, null, null, null, "kafka",
            Map.of(), System.currentTimeMillis() + 300000, System.currentTimeMillis(),
            System.currentTimeMillis(), System.currentTimeMillis() + 86400);
        AwaitCreateResult mockCreateResult = new AwaitCreateResult(mockRecord, false);

        when(awaitCoordinator.createOrGet(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(5),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(List.of("first", "second")),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull()))
            .thenReturn(Uni.createFrom().item(mockCreateResult));
        when(awaitCoordinator.dispatch(testDescriptor, mockRecord))
            .thenReturn(Uni.createFrom().item(mockRecord));

        assertThrows(
            AwaitSuspendedException.class,
            () -> support.awaitManyToMany(testDescriptor, Multi.createFrom().items("first", "second"))
                .collect().asList()
                .await().indefinitely());

        org.mockito.Mockito.verify(awaitCoordinator).createOrGet(
            org.mockito.ArgumentMatchers.eq(testDescriptor),
            org.mockito.ArgumentMatchers.eq("tenant1"),
            org.mockito.ArgumentMatchers.eq("exec123"),
            org.mockito.ArgumentMatchers.eq(5),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(List.of("first", "second")),
            org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull());
        org.mockito.Mockito.verify(awaitCoordinator, never()).createOrGetItem(
            any(), any(), any(), anyInt(), any(), any(), any(), anyInt(), any(), any());
    }

    @Test
    void awaitManyToOneRejectsOversizedMaterializedInput() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.awaitAggregateMaxInputItems()).thenReturn(1);
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant1", "exec123", 6));
        AwaitStepDescriptor testDescriptor = new AwaitStepDescriptor(
            "batch-review",
            String.class.getName(),
            String.class.getName(),
            "MANY_TO_ONE",
            Duration.ofMinutes(5),
            "signedResumeToken",
            "kafka",
            Map.of(),
            List.of());

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            () -> support.awaitManyToOne(testDescriptor, Multi.createFrom().items("first", "second"))
                .await().indefinitely());

        assertTrue(error.getMessage().contains("pipeline.orchestrator.await-aggregate-max-input-items=1"));
        org.mockito.Mockito.verify(awaitCoordinator, never()).createOrGet(
            any(), any(), any(), anyInt(), any(), any(), any(), any());
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
