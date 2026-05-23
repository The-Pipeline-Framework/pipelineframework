package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static AwaitCoordinator coordinator(InMemoryAwaitInteractionStore store) {
        AwaitCoordinator coordinator = new AwaitCoordinator();
        coordinator.stores = new SimpleInstance<>(List.<AwaitInteractionStore>of(store));
        coordinator.adapters = new SimpleInstance<>(List.<AwaitTransportAdapter<?>>of());
        coordinator.resumeTokenService = new AwaitResumeTokenService("secret-value-for-tests");
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
