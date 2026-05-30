package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.UnsatisfiedResolutionException;
import jakarta.enterprise.util.TypeLiteral;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

class AwaitCoordinatorCompletionTest {

    @Test
    void createOrGetNormalizesProtobufRequestPayload() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        DescriptorProtos.FileDescriptorProto payload = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("checkout.proto")
            .setPackage("org.pipelineframework.checkout")
            .build();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "FraudCheck",
            DescriptorProtos.FileDescriptorProto.class.getName(),
            "com.example.Decision",
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of("name"));

        AwaitCreateResult result = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            payload,
            null,
            null).await().indefinitely();

        assertTrue(result.record().requestPayload() instanceof Map<?, ?>);
        Map<?, ?> requestPayload = (Map<?, ?>) result.record().requestPayload();
        assertEquals("checkout.proto", requestPayload.get("name"));
        assertEquals("FraudCheck:name=checkout.proto", result.record().idempotencyKey());
    }

    @Test
    void validatesResumeTokenBeforeCompletion() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord record = store.createOrGet(createCommand(20_000L)).await().indefinitely().record();
        String token = coordinator.resumeTokenService.sign(record, 10_000L);

        AwaitCompletionResult result = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            record.interactionId(),
            null,
            token,
            "completion-1",
            java.util.Map.of("decision", "approved"),
            "alice",
            11_000L)).await().indefinitely();

        assertEquals(AwaitInteractionStatus.COMPLETED, result.record().status());
        assertEquals("alice", result.record().actor());
    }

    @Test
    void rejectsTokenForWrongInteraction() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord first = store.createOrGet(createCommand("idem-1", "corr-1", 20_000L)).await().indefinitely().record();
        AwaitInteractionRecord second = store.createOrGet(createCommand("idem-2", "corr-2", 20_000L)).await().indefinitely().record();
        String token = coordinator.resumeTokenService.sign(first, 10_000L);

        assertThrows(IllegalArgumentException.class, () -> coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            second.interactionId(),
            null,
            token,
            "completion-1",
            java.util.Map.of("decision", "approved"),
            "alice",
            11_000L)).await().indefinitely());
    }

    @Test
    void duplicateTokenCompletionRemainsIdempotent() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord record = store.createOrGet(createCommand(20_000L)).await().indefinitely().record();
        String token = coordinator.resumeTokenService.sign(record, 10_000L);
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            record.interactionId(),
            null,
            token,
            "completion-1",
            java.util.Map.of("decision", "approved"),
            "alice",
            11_000L);

        coordinator.complete(command).await().indefinitely();
        AwaitCompletionResult duplicate = coordinator.complete(command).await().indefinitely();

        assertTrue(duplicate.duplicate());
    }

    @Test
    void duplicateItemCompletionDoesNotOverCountAwaitUnit() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitCreateResult created = coordinator.createOrGetItem(
            descriptor("AwaitPaymentProvider"),
            "tenant-1",
            "exec-1",
            1,
            "record-1",
            java.util.Map.of("paymentRecordId", "record-1"),
            "unit-1",
            0,
            null,
            null).await().indefinitely();
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            created.record().interactionId(),
            null,
            "completion-1",
            java.util.Map.of("status", "APPROVED"),
            "provider",
            11_000L);

        AwaitCompletionResult first = coordinator.complete(command).await().indefinitely();
        coordinator.recordCompletion(first.record(), 11_000L).await().indefinitely();
        AwaitCompletionResult duplicate = coordinator.complete(command).await().indefinitely();
        AwaitUnitRecord afterDuplicate = coordinator.recordCompletion(duplicate.record(), 12_000L).await().indefinitely();
        AwaitUnitRecord completed = coordinator.markDispatchComplete("tenant-1", "unit-1", 1, 13_000L).await().indefinitely();

        assertFalse(first.duplicate());
        assertTrue(duplicate.duplicate());
        assertEquals(1, afterDuplicate.completedItemCount());
        assertEquals(1, completed.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, completed.status());
    }

    @Test
    void retryItemInteractionWithSameIndexDoesNotOverCountAwaitUnit() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitCreateResult first = coordinator.createOrGetItem(
            descriptor("AwaitPaymentProvider"),
            "tenant-1",
            "exec-1",
            1,
            "record-1",
            java.util.Map.of("paymentRecordId", "record-1"),
            "unit-1",
            0,
            null,
            null).await().indefinitely();
        AwaitCreateResult retried = coordinator.createOrGetItem(
            descriptor("AwaitPaymentProvider"),
            "tenant-1",
            "exec-1",
            1,
            "record-1-retry",
            java.util.Map.of("paymentRecordId", "record-1-retry"),
            "unit-1",
            0,
            null,
            null).await().indefinitely();

        AwaitCompletionResult firstCompleted = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            first.record().interactionId(),
            null,
            "completion-1",
            java.util.Map.of("status", "APPROVED"),
            "provider",
            11_000L)).await().indefinitely();
        AwaitCompletionResult retryCompleted = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            retried.record().interactionId(),
            null,
            "completion-2",
            java.util.Map.of("status", "APPROVED"),
            "provider",
            12_000L)).await().indefinitely();

        AwaitUnitRecord afterFirst = coordinator.recordCompletion(firstCompleted.record(), 11_000L).await().indefinitely();
        AwaitUnitRecord afterRetry = coordinator.recordCompletion(retryCompleted.record(), 12_000L).await().indefinitely();
        AwaitUnitRecord completed = coordinator.markDispatchComplete("tenant-1", "unit-1", 1, 13_000L).await().indefinitely();

        assertFalse(firstCompleted.duplicate());
        assertFalse(retryCompleted.duplicate());
        assertEquals(1, afterFirst.completedItemCount());
        assertEquals(1, afterRetry.completedItemCount());
        assertEquals(1, completed.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, completed.status());
    }

    @Test
    void staleTerminalInteractionIsRejectedBeforeTokenCompletion() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord record = store.createOrGet(createCommand(20_000L)).await().indefinitely().record();
        String token = coordinator.resumeTokenService.sign(record, 10_000L);
        store.cancel("tenant-1", record.interactionId(), record.version(), "cancelled", 12_000L)
            .await().indefinitely();

        assertThrows(AwaitInteractionTerminalException.class, () -> coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            record.interactionId(),
            null,
            token,
            "completion-1",
            java.util.Map.of("decision", "approved"),
            "alice",
            13_000L)).await().indefinitely());
    }

    @Test
    void rejectsExpiredResumeToken() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord record = store.createOrGet(createCommand(20_000L)).await().indefinitely().record();
        String token = coordinator.resumeTokenService.sign(record, 10_000L);

        assertThrows(AwaitResumeTokenRejectedException.class, () -> coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            record.interactionId(),
            null,
            token,
            "completion-1",
            java.util.Map.of("decision", "approved"),
            "alice",
            21_000L)).await().indefinitely());
    }

    @Test
    void completeNormalizesProtobufResponsePayload() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord record = store.createOrGet(createCommand(20_000L)).await().indefinitely().record();
        DescriptorProtos.FileDescriptorProto payload = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("approval.proto")
            .build();

        AwaitCompletionResult result = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            record.interactionId(),
            null,
            "completion-1",
            payload,
            "alice",
            11_000L)).await().indefinitely();

        assertTrue(result.record().responsePayload() instanceof Map<?, ?>);
        assertEquals("approval.proto", ((Map<?, ?>) result.record().responsePayload()).get("name"));
    }

    @Test
    void loadResumePayloadCoercesStoredSnapshotToDeclaredOutputType() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "DescriptorApproval",
            DescriptorProtos.FileDescriptorProto.class.getName(),
            DescriptorProtos.FileDescriptorProto.class.getName(),
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of());

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            DescriptorProtos.FileDescriptorProto.newBuilder().setName("request.proto").build(),
            null,
            null).await().indefinitely();
        AwaitCompletionResult completed = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            created.record().interactionId(),
            null,
            "completion-1",
            DescriptorProtos.FileDescriptorProto.newBuilder().setName("approval.proto").build(),
            "alice",
            11_000L)).await().indefinitely();
        coordinator.recordCompletion(completed.record(), 11_000L).await().indefinitely();

        Object payload = coordinator.loadResumePayload("tenant-1", created.record().unitId()).await().indefinitely();

        assertTrue(payload instanceof DescriptorProtos.FileDescriptorProto);
        assertEquals("approval.proto", ((DescriptorProtos.FileDescriptorProto) payload).getName());
    }

    @Test
    void completeRejectsOversizedMaterializedOutputUnit() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        PipelineOrchestratorConfig config = org.mockito.Mockito.mock(PipelineOrchestratorConfig.class);
        org.mockito.Mockito.when(config.awaitAggregateMaxOutputItems()).thenReturn(1);
        AwaitCoordinator coordinator = coordinator(store, config);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "BatchApproval",
            List.class.getName(),
            List.class.getName(),
            "MANY_TO_MANY",
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of());

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            List.of("input-a", "input-b"),
            null,
            null).await().indefinitely();
        AwaitCompletionCommand completion = new AwaitCompletionCommand(
            "tenant-1",
            created.record().interactionId(),
            null,
            "completion-1",
            List.of("output-a", "output-b"),
            "alice",
            11_000L);

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            () -> coordinator.complete(completion).await().indefinitely());

        assertTrue(error.getMessage().contains("pipeline.orchestrator.await-aggregate-max-output-items=1"));
    }

    private static AwaitCoordinator coordinator(InMemoryAwaitInteractionStore store) {
        return coordinator(store, null);
    }

    private static AwaitCoordinator coordinator(InMemoryAwaitInteractionStore store, PipelineOrchestratorConfig config) {
        AwaitCoordinator coordinator = new AwaitCoordinator();
        coordinator.interactionStores = new SimpleInstance<>(List.<AwaitInteractionStore>of(store));
        coordinator.unitStores = new SimpleInstance<>(List.of(new InMemoryAwaitUnitStore()));
        coordinator.adapters = new SimpleInstance<>(List.<AwaitTransportAdapter<?>>of());
        coordinator.resumeTokenService = new AwaitResumeTokenService("secret-value-for-tests");
        coordinator.orchestratorConfig = config;
        return coordinator;
    }

    private static AwaitCreateCommand createCommand(long deadlineEpochMs) {
        return createCommand("idem-1", "corr-1", deadlineEpochMs);
    }

    private static AwaitCreateCommand createCommand(String idempotencyKey, String correlationId, long deadlineEpochMs) {
        return new AwaitCreateCommand(
            "tenant-1",
            "exec-1",
            "FraudCheck",
            1,
            "com.example.Decision",
            "cause-1",
            idempotencyKey,
            correlationId,
            java.util.Map.of("orderId", "o-1"),
            null,
            null,
            "webhook",
            10_000L,
            deadlineEpochMs,
            9_999_999_999L);
    }

    private static AwaitStepDescriptor descriptor(String stepId) {
        return new AwaitStepDescriptor(
            stepId,
            java.util.Map.class.getName(),
            java.util.Map.class.getName(),
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of("paymentRecordId"));
    }

    private static final class SimpleInstance<T> implements Instance<T> {
        private final List<T> items;

        private SimpleInstance(List<T> items) {
            this.items = items;
        }

        @Override
        public Instance<T> select(Annotation... qualifiers) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isUnsatisfied() {
            return items.isEmpty();
        }

        @Override
        public boolean isAmbiguous() {
            return items.size() > 1;
        }

        @Override
        public void destroy(T instance) {
        }

        @Override
        public Handle<T> getHandle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<? extends Handle<T>> handles() {
            return List.of();
        }

        @Override
        public java.util.Iterator<T> iterator() {
            return items.iterator();
        }

        @Override
        public T get() {
            if (items.isEmpty()) {
                throw new UnsatisfiedResolutionException();
            }
            return items.get(0);
        }

        @Override
        public java.util.stream.Stream<T> stream() {
            return items.stream();
        }
    }
}
