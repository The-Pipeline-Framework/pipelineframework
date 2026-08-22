package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.annotation.Annotation;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.UnsatisfiedResolutionException;
import jakarta.enterprise.util.TypeLiteral;

import com.google.protobuf.DescriptorProtos;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;

class AwaitCoordinatorCompletionTest {

    @Test
    void createOrGetPersistsExplicitOriginTraceMetadata() {
        AwaitCoordinator coordinator = coordinator(new InMemoryAwaitInteractionStore());
        Map<String, Object> traceMetadata = Map.of(
            "tpf.trace.id", "0123456789abcdef0123456789abcdef",
            "tpf.trace.span_id", "0123456789abcdef",
            "tpf.trace.flags", "01");

        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            "tenant-1", "exec-1", 1, AwaitContinuationMode.LIVE_IF_SUPPORTED,
            TerminalOutputOwnership.TRANSITION_WORKER, traceMetadata));
        try {
            AwaitInteractionRecord created = coordinator.createOrGet(
                descriptor("FraudCheck"),
                "tenant-1",
                "exec-1",
                1,
                "cause-1",
                Map.of("orderId", "o-1"),
                "alice",
                "fraud-review").await().indefinitely().record();

            assertEquals(traceMetadata, created.transportMetadata());
        } finally {
            AwaitExecutionContextHolder.clear();
        }
    }

    @Test
    void scalarCompletionDoesNotReadAwaitUnitForAggregateOutputLimiting() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord created = coordinator.createOrGet(
            descriptor("FraudCheck"),
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            Map.of("orderId", "o-1"),
            null,
            null).await().indefinitely().record();
        AwaitUnitStore units = mock(AwaitUnitStore.class);
        coordinator.unitStores = new SimpleInstance<>(List.of(units));

        AwaitCompletionResult completion = coordinator.complete(new AwaitCompletionCommand(
                created.tenantId(),
                created.interactionId(),
                null,
                "completion-1",
                Map.of("decision", "approved"),
                "provider",
                2_000L))
            .await().indefinitely();

        assertEquals(AwaitInteractionStatus.COMPLETED, completion.record().status());
        verify(units, never()).get(anyString(), anyString());
    }

    @Test
    void createOrGetRestoresCanonicalRequestBeforeInputToTransportMapping() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AtomicReference<CanonicalRequest> adapterInput = new AtomicReference<>();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "CanonicalRequest",
            CanonicalRequest.class.getName(),
            String.class.getName(),
            "ONE_TO_ONE",
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of("id"),
            Map.class.getName(),
            String.class.getName(),
            value -> {
                CanonicalRequest canonical = (CanonicalRequest) value;
                adapterInput.set(canonical);
                return Map.of("id", canonical.id().toString(), "sourcePath", canonical.sourcePath().toString());
            },
            Function.identity());
        UUID id = UUID.fromString("8ee4c940-15a8-42a4-8f4e-af5a898c37a1");

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            Map.of("id", id.toString(), "sourcePath", "/app/test-e2e"),
            null,
            null).await().indefinitely();

        assertEquals(new CanonicalRequest(id, Path.of("/app/test-e2e")), created.record().requestPayload());
        assertTrue(adapterInput.get() == null);
        AtomicReference<Object> dispatchedPayload = new AtomicReference<>();
        coordinator.adapters = new SimpleInstance<>(List.of(new AwaitTransportAdapter<>() {
            @Override
            public String type() {
                return "interaction-api";
            }

            @Override
            public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
                dispatchedPayload.set(request.payload());
                return Uni.createFrom().item(new AwaitDispatchResult(Map.of()));
            }
        }));

        coordinator.dispatch(descriptor, created.record()).await().indefinitely();

        assertEquals(new CanonicalRequest(id, Path.of("/app/test-e2e")), adapterInput.get());
        assertEquals(Map.of("id", id.toString(), "sourcePath", "/app/test-e2e"), dispatchedPayload.get());
    }

    @Test
    void createOrGetRetainsProtobufRequestPayloadUntilDispatch() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        DescriptorProtos.FileDescriptorProto payload = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("checkout.proto")
            .setPackage("org.pipelineframework.checkout")
            .build();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "ProtoFraudCheck",
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

        assertEquals(payload, result.record().requestPayload());
        assertEquals("ProtoFraudCheck:name=checkout.proto", result.record().idempotencyKey());
    }

    @Test
    void suspensionSnapshotNormalizesProtobufPayloadsForPortableTransition() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        DescriptorProtos.FileDescriptorProto payload = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("checkout.proto")
            .setPackage("org.pipelineframework.checkout")
            .build();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "ProtoFraudCheck",
            DescriptorProtos.FileDescriptorProto.class.getName(),
            DescriptorProtos.FileDescriptorProto.class.getName(),
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of("name"));
        AwaitInteractionRecord created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            payload,
            null,
            null).await().indefinitely().record();
        AwaitInteractionRecord rawProtobufPayload = new AwaitInteractionRecord(
            created.tenantId(), created.executionId(), created.stepId(), created.stepIndex(), created.outputType(),
            "portable-protobuf", "portable-protobuf", created.causationId(), "portable-protobuf",
            created.version(), created.status(), payload, payload, created.unitId(), created.itemIndex(),
            created.actor(), created.assignee(), created.group(), created.transportType(), created.transportMetadata(),
            created.deadlineEpochMs(), created.createdAtEpochMs(), created.updatedAtEpochMs(), created.ttlEpochS(),
            created.transportOutputType());
        store.importRecord(rawProtobufPayload).await().indefinitely();

        TransitionAwaitSuspension snapshot = coordinator.suspensionSnapshot(
            new AwaitSuspendedException("tenant-1", "exec-1", created.unitId(), created.stepIndex()))
            .await().indefinitely();
        AwaitInteractionRecord portable = snapshot.interactions().stream()
            .filter(interaction -> interaction.interactionId().equals("portable-protobuf"))
            .findFirst()
            .orElseThrow();

        assertTrue(portable.requestPayload() instanceof Map<?, ?>);
        assertTrue(portable.responsePayload() instanceof Map<?, ?>);
        assertDoesNotThrow(() -> PipelineJson.mapper().writeValueAsString(snapshot));
    }

    @Test
    void createOrGetDerivesIdentityFromCanonicalRequestAndPersistsCanonicalRequest() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "V3PaymentProvider",
            Map.class.getName(),
            Map.class.getName(),
            "ONE_TO_ONE",
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of("id"),
            "com.example.transport.PaymentRequest",
            "com.example.transport.PaymentStatus",
            value -> Map.of("wireRequest", value),
            value -> value);

        AwaitCreateResult result = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            Map.of("id", "canonical-payment-1"),
            null,
            null).await().indefinitely();

        assertEquals("V3PaymentProvider:id=canonical-payment-1", result.record().idempotencyKey());
        assertEquals(Map.of("id", "canonical-payment-1"), result.record().requestPayload());
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
    void completesUsingResumeTokenOnly() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitInteractionRecord record = store.createOrGet(createCommand(20_000L)).await().indefinitely().record();
        String token = coordinator.resumeTokenService.sign(record, 10_000L);

        AwaitCompletionResult result = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            null,
            null,
            token,
            "completion-token-only",
            java.util.Map.of("decision", "approved"),
            "alice",
            11_000L)).await().indefinitely();

        assertEquals(AwaitInteractionStatus.COMPLETED, result.record().status());
        assertEquals(record.interactionId(), result.record().interactionId());
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
    void terminalItemReplayIsOrderIndependentAndDoesNotDoubleCountAwaitUnit() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        List<AwaitCreateResult> created = List.of(
            item(coordinator, "record-2", 2),
            item(coordinator, "record-0", 0),
            item(coordinator, "record-1", 1));

        for (AwaitCreateResult result : created) {
            coordinator.complete(new AwaitCompletionCommand(
                "tenant-1",
                result.record().interactionId(),
                null,
                "completion-" + result.record().itemIndex(),
                Map.of("status", "APPROVED"),
                "provider",
                11_000L + result.record().itemIndex())).await().indefinitely();
        }

        coordinator.reconcileCompletedItemInteractions("tenant-1", "unit-1", 20_000L).await().indefinitely();
        coordinator.reconcileCompletedItemInteractions("tenant-1", "unit-1", 21_000L).await().indefinitely();
        AwaitUnitRecord completed = coordinator.markDispatchComplete("tenant-1", "unit-1", 3, 22_000L)
            .await().indefinitely();

        assertEquals(3, completed.completedItemCount());
        assertEquals(java.util.Set.of("item:0", "item:1", "item:2"), completed.completedItemKeys());
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
    void failsDeterministicallyWhenDurableInteractionStepCannotResolveADescriptor() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        coordinator.descriptorFactory = new AwaitStepDescriptorFactory();
        AwaitInteractionRecord record = store.createOrGet(createCommand(20_000L)).await().indefinitely().record();

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> coordinator.complete(
            new AwaitCompletionCommand(
                "tenant-1",
                record.interactionId(),
                null,
                "completion-1",
                Map.of("decision", "approved"),
                "alice",
                11_000L)).await().indefinitely());

        assertTrue(error.getMessage().contains("execution exec-1"));
        assertTrue(error.getMessage().contains("interaction " + record.interactionId()));
        assertTrue(error.getMessage().contains("stepId=FraudCheck"));
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
    void resumePayloadUsesTransportTypeBeforeApplyingCanonicalAdapter() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "CanonicalDecision",
            Map.class.getName(),
            CanonicalDecision.class.getName(),
            "ONE_TO_ONE",
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of(),
            Map.class.getName(),
            Map.class.getName(),
            java.util.function.Function.identity(),
            payload -> new CanonicalDecision(((Map<?, ?>) payload).get("status").toString()));

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            Map.of("orderId", "o-1"),
            null,
            null).await().indefinitely();
        AwaitCompletionResult completed = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            created.record().interactionId(),
            null,
            null,
            Map.of("status", "APPROVED"),
            "alice",
            11_000L)).await().indefinitely();

        assertEquals(new CanonicalDecision("APPROVED"), completed.record().responsePayload());
        assertEquals(new CanonicalDecision("APPROVED"), coordinator.resumePayload(completed.record()));
    }

    @Test
    void requestAwareCompletionProjectsCanonicalRequestAndActorPayloadBeforePersistence() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "ConfirmedSelection",
            PendingSelection.class.getName(),
            ConfirmedSelection.class.getName(),
            "ONE_TO_ONE",
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of("documentId"),
            PendingSelection.class.getName(),
            SelectionChoice.class.getName(),
            java.util.function.Function.identity(),
            java.util.function.Function.identity(),
            "confirmed-selection-v1",
            (request, completion, metadata) -> {
                PendingSelection pending = (PendingSelection) request;
                SelectionChoice choice = (SelectionChoice) completion;
                return new ConfirmedSelection(
                    pending.documentId(),
                    pending.recommendedPropertyId(),
                    choice.propertyId(),
                    metadata.completedAt());
            },
            true);
        coordinator.descriptorFactory.register(descriptor);

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            new PendingSelection("invoice-1", "property-a"),
            "alice",
            "property-review").await().indefinitely();
        AwaitCompletionResult completed = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            created.record().interactionId(),
            created.record().correlationId(),
            "completion-1",
            Map.of("propertyId", "property-b"),
            "alice",
            11_000L)).await().indefinitely();

        ConfirmedSelection expected = new ConfirmedSelection(
            "invoice-1", "property-a", "property-b", java.time.Instant.ofEpochMilli(11_000L));
        assertEquals(expected, completed.record().responsePayload());
        assertEquals(expected, coordinator.resumePayload(completed.record()));
        assertEquals(new PendingSelection("invoice-1", "property-a"), completed.record().requestPayload());
    }

    @Test
    void createUsesThePreviouslyRegisteredProjectorForRepeatedStepIds() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor original = requestAwareDescriptor(
            "RepeatedSelection", "selection-projector-v1", new OriginalSelectionProjector());
        AwaitStepDescriptor replacement = requestAwareDescriptor(
            "RepeatedSelection", "selection-projector-v2", new ChangedSelectionProjector());

        coordinator.createOrGet(
            original, "tenant-1", "exec-1", 1, "cause-1",
            new PendingSelection("invoice-1", "property-a"), "alice", "property-review").await().indefinitely();
        AwaitCreateResult created = coordinator.createOrGet(
            replacement, "tenant-1", "exec-2", 1, "cause-2",
            new PendingSelection("invoice-2", "property-b"), "alice", "property-review").await().indefinitely();

        assertEquals("selection-projector-v1",
            created.record().transportMetadata().get("tpf.await.completion.projector"));
    }

    @Test
    void requestAwareSuspensionRoundTripDoesNotProjectCanonicalCompletionTwice() throws Exception {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        java.util.concurrent.atomic.AtomicInteger projections = new java.util.concurrent.atomic.AtomicInteger();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "PortableSelection", PendingSelection.class.getName(), ConfirmedSelection.class.getName(),
            "ONE_TO_ONE", java.time.Duration.ofMinutes(10), "interactionId", "interaction-api", Map.of(),
            List.of("documentId"), PendingSelection.class.getName(), SelectionChoice.class.getName(),
            Function.identity(), Function.identity(), "portable-selection-v1",
            (request, completion, metadata) -> {
                projections.incrementAndGet();
                PendingSelection pending = (PendingSelection) request;
                SelectionChoice choice = (SelectionChoice) completion;
                return new ConfirmedSelection(pending.documentId(), pending.recommendedPropertyId(),
                    choice.propertyId(), metadata.completedAt());
            }, true);
        AwaitCreateResult created = coordinator.createOrGet(
            descriptor, "tenant-1", "exec-1", 1, "cause-1",
            new PendingSelection("invoice-1", "property-a"), "alice", "property-review").await().indefinitely();
        coordinator.complete(new AwaitCompletionCommand(
            "tenant-1", created.record().interactionId(), created.record().correlationId(), "completion-1",
            Map.of("propertyId", "property-b"), "alice", 11_000L)).await().indefinitely();

        TransitionAwaitSuspension exported = coordinator.suspensionSnapshot(
            new AwaitSuspendedException("tenant-1", "exec-1", created.record().unitId(), 1))
            .await().indefinitely();
        String json = PipelineJson.mapper().writeValueAsString(exported);
        TransitionAwaitSuspension imported = PipelineJson.mapper().readValue(json, TransitionAwaitSuspension.class);
        InMemoryAwaitInteractionStore restartedStore = new InMemoryAwaitInteractionStore();
        AwaitCoordinator restarted = coordinator(restartedStore);
        restarted.descriptorFactory.register(descriptor);
        restarted.importSuspension(imported).await().indefinitely();
        AwaitInteractionRecord restored = restartedStore.get("tenant-1", created.record().interactionId())
            .await().indefinitely().orElseThrow();

        assertEquals(new ConfirmedSelection(
            "invoice-1", "property-a", "property-b", java.time.Instant.ofEpochMilli(11_000L)),
            restarted.resumePayload(restored));
        assertEquals(1, projections.get());
    }

    @Test
    void dispatchMetadataCannotOverwritePinnedCompletionProjector() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = requestAwareDescriptor(new OriginalSelectionProjector());
        coordinator.descriptorFactory.register(descriptor);
        AwaitCreateResult created = coordinator.createOrGet(
            descriptor, "tenant-1", "exec-1", 1, "cause-1",
            new PendingSelection("invoice-1", "property-a"), "alice", "property-review").await().indefinitely();
        coordinator.adapters = new SimpleInstance<>(List.of(new AwaitTransportAdapter<>() {
            @Override
            public String type() {
                return "interaction-api";
            }

            @Override
            public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
                return Uni.createFrom().item(new AwaitDispatchResult(Map.of(
                    "tpf.await.completion.projector", ChangedSelectionProjector.class.getName())));
            }
        }));

        AwaitInteractionRecord dispatched = coordinator.dispatch(descriptor, created.record()).await().indefinitely();

        assertEquals(OriginalSelectionProjector.class.getName(),
            dispatched.transportMetadata().get("tpf.await.completion.projector"));
        assertEquals(OriginalSelectionProjector.class.getName(),
            store.get("tenant-1", created.record().interactionId()).await().indefinitely().orElseThrow()
                .transportMetadata().get("tpf.await.completion.projector"));
    }

    @Test
    void normalCompletionIgnoresReservedProjectorMetadataFromTraceAndAdapter() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = descriptor("NormalSelection");
        coordinator.adapters = new SimpleInstance<>(List.of(new AwaitTransportAdapter<>() {
            @Override
            public String type() {
                return "interaction-api";
            }

            @Override
            public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
                return Uni.createFrom().item(new AwaitDispatchResult(Map.of(
                    "tpf.await.completion.projector", ChangedSelectionProjector.class.getName())));
            }
        }));
        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            "tenant-1", "exec-1", 1, AwaitContinuationMode.LIVE_IF_SUPPORTED,
            TerminalOutputOwnership.TRANSITION_WORKER,
            Map.of("tpf.await.completion.projector", ChangedSelectionProjector.class.getName())));
        try {
            AwaitCreateResult created = coordinator.createOrGet(
                descriptor, "tenant-1", "exec-1", 1, "cause-1", Map.of("invoiceId", "invoice-1"),
                "alice", "property-review").await().indefinitely();
            assertFalse(created.record().transportMetadata().containsKey("tpf.await.completion.projector"));

            AwaitInteractionRecord dispatched = coordinator.dispatch(descriptor, created.record()).await().indefinitely();
            assertFalse(dispatched.transportMetadata().containsKey("tpf.await.completion.projector"));

            AwaitCompletionResult completed = coordinator.complete(new AwaitCompletionCommand(
                "tenant-1", created.record().interactionId(), created.record().correlationId(), "completion-1",
                Map.of("decision", "approved"), "alice", 11_000L)).await().indefinitely();
            assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());
        } finally {
            AwaitExecutionContextHolder.clear();
        }
    }

    @Test
    void requestAwareProjectionFailureLeavesInteractionWaiting() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "RejectedSelection",
            PendingSelection.class.getName(),
            ConfirmedSelection.class.getName(),
            "ONE_TO_ONE",
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of("documentId"),
            PendingSelection.class.getName(),
            SelectionChoice.class.getName(),
            java.util.function.Function.identity(),
            java.util.function.Function.identity(),
            "rejected-selection-v1",
            (request, completion, metadata) -> {
                throw new IllegalArgumentException("selection is no longer valid");
            },
            true);
        coordinator.descriptorFactory.register(descriptor);

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            new PendingSelection("invoice-1", "property-a"),
            "alice",
            "property-review").await().indefinitely();

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> coordinator.complete(
            new AwaitCompletionCommand(
                "tenant-1",
                created.record().interactionId(),
                created.record().correlationId(),
                "completion-1",
                Map.of("propertyId", "property-b"),
                "alice",
                11_000L)).await().indefinitely());

        assertEquals("selection is no longer valid", failure.getMessage());
        AwaitInteractionRecord stored = store.get("tenant-1", created.record().interactionId())
            .await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.WAITING, stored.status());
        assertNull(stored.responsePayload());
    }

    @Test
    void requestAwareCompletionRejectsAChangedProjectorAfterRestart() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator firstRuntime = coordinator(store);
        AwaitStepDescriptor original = requestAwareDescriptor(new OriginalSelectionProjector());
        firstRuntime.descriptorFactory.register(original);
        AwaitCreateResult created = firstRuntime.createOrGet(
            original, "tenant-1", "exec-1", 1, "cause-1",
            new PendingSelection("invoice-1", "property-a"), "alice", "property-review").await().indefinitely();

        AwaitCoordinator restartedRuntime = coordinator(store);
        restartedRuntime.descriptorFactory.register(requestAwareDescriptor(new ChangedSelectionProjector()));
        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> restartedRuntime.complete(
            new AwaitCompletionCommand(
                "tenant-1", created.record().interactionId(), created.record().correlationId(), "completion-1",
                Map.of("propertyId", "property-b"), "alice", 11_000L)).await().indefinitely());

        assertTrue(failure.getMessage().contains("pinned completion projector"));
        assertEquals(AwaitInteractionStatus.WAITING,
            store.get("tenant-1", created.record().interactionId()).await().indefinitely().orElseThrow().status());
    }

    private static AwaitStepDescriptor requestAwareDescriptor(
        AwaitCompletionProjector<PendingSelection, SelectionChoice, ConfirmedSelection> projector
    ) {
        return requestAwareDescriptor(
            "RestartedSelection", projector.getClass().getName(), projector);
    }

    private static AwaitStepDescriptor requestAwareDescriptor(
        String stepId,
        String projectorId,
        AwaitCompletionProjector<PendingSelection, SelectionChoice, ConfirmedSelection> projector
    ) {
        @SuppressWarnings("unchecked")
        AwaitCompletionProjector<Object, Object, Object> untyped =
            (AwaitCompletionProjector<Object, Object, Object>) (AwaitCompletionProjector<?, ?, ?>) projector;
        return new AwaitStepDescriptor(
            stepId, PendingSelection.class.getName(), ConfirmedSelection.class.getName(),
            "ONE_TO_ONE", java.time.Duration.ofMinutes(10), "interactionId", "interaction-api", Map.of(),
            List.of("documentId"), PendingSelection.class.getName(), SelectionChoice.class.getName(),
            Function.identity(), Function.identity(), projectorId, untyped, true);
    }

    @Test
    void acceptsLegacyV2NestedProtobufSourceTypeIdentity() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        String binaryType = DescriptorProtos.FileDescriptorProto.class.getName();
        String sourceType = DescriptorProtos.FileDescriptorProto.class.getCanonicalName();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "LegacyProtobufDecision", binaryType, binaryType, java.time.Duration.ofMinutes(10),
            "interactionId", "interaction-api", Map.of(), List.of());

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor, "tenant-1", "exec-1", 1, "cause-1", DescriptorProtos.FileDescriptorProto.getDefaultInstance(),
            null, null).await().indefinitely();
        AwaitCompletionResult completed = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1", created.record().interactionId(), null, null,
            DescriptorProtos.FileDescriptorProto.getDefaultInstance(), "alice", 11_000L)).await().indefinitely();
        AwaitInteractionRecord record = completed.record();
        AwaitInteractionRecord legacySourceNamed = new AwaitInteractionRecord(
            record.tenantId(), record.executionId(), record.stepId(), record.stepIndex(), sourceType,
            record.interactionId(), record.correlationId(), record.causationId(), record.idempotencyKey(),
            record.version(), record.status(), record.requestPayload(), record.responsePayload(), record.unitId(),
            record.itemIndex(), record.actor(), record.assignee(), record.group(), record.transportType(),
            record.transportMetadata(), record.deadlineEpochMs(), record.createdAtEpochMs(), record.updatedAtEpochMs(),
            record.ttlEpochS(), sourceType);

        assertEquals(DescriptorProtos.FileDescriptorProto.getDefaultInstance(), coordinator.resumePayload(legacySourceNamed));
    }

    @Test
    void defersLegacySemanticPayloadConversionUntilResume() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "LegacyDecision",
            Map.class.getName(),
            StrictDecision.class.getName(),
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of());
        coordinator.descriptorFactory.register(descriptor);

        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            1,
            "cause-1",
            Map.of("orderId", "o-1"),
            null,
            null).await().indefinitely();
        AwaitCompletionResult completed = coordinator.complete(new AwaitCompletionCommand(
            "tenant-1",
            created.record().interactionId(),
            null,
            "completion-1",
            Map.of("orderId", "not-a-uuid"),
            "alice",
            11_000L)).await().indefinitely();
        coordinator.recordCompletion(completed.record(), 11_000L).await().indefinitely();

        assertTrue(completed.record().responsePayload() instanceof Map<?, ?>);
        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> coordinator.loadResumePayload("tenant-1", created.record().unitId()).await().indefinitely());
        assertTrue(error.getMessage().contains("Failed converting await payload"));
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

    @Test
    void dispatchReturnsCompletedRecordWhenCompletionWinsMetadataRace() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCoordinator coordinator = coordinator(store);
        coordinator.adapters = new SimpleInstance<>(List.of(new FastCompletionAdapter(coordinator)));
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "FastAwait",
            java.util.Map.class.getName(),
            java.util.Map.class.getName(),
            java.time.Duration.ofMinutes(10),
            "interactionId",
            "fast-completion",
            Map.of(),
            List.of("paymentRecordId"));
        AwaitCreateResult created = coordinator.createOrGet(
            descriptor,
            "tenant-1",
            "exec-1",
            2,
            "cause-1",
            java.util.Map.of("paymentRecordId", "p-1"),
            null,
            null).await().indefinitely();

        AwaitInteractionRecord dispatched = coordinator.dispatch(descriptor, created.record()).await().indefinitely();

        assertEquals(AwaitInteractionStatus.COMPLETED, dispatched.status());
        assertEquals(
            AwaitInteractionStatus.COMPLETED,
            store.get("tenant-1", created.record().interactionId()).await().indefinitely().orElseThrow().status());
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
        coordinator.descriptorFactory = new AwaitStepDescriptorFactory();
        coordinator.descriptorFactory.register(descriptor("FraudCheck"));
        return coordinator;
    }

    private static AwaitCreateResult item(AwaitCoordinator coordinator, String recordId, int itemIndex) {
        return coordinator.createOrGetItem(
            descriptor("AwaitPaymentProvider"),
            "tenant-1",
            "exec-1",
            1,
            recordId,
            Map.of("paymentRecordId", recordId),
            "unit-1",
            itemIndex,
            null,
            null).await().indefinitely();
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
            Map.class.getName(),
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

    private record StrictDecision(java.util.UUID orderId) {
    }

    private record CanonicalRequest(UUID id, Path sourcePath) {
    }

    private record CanonicalDecision(String status) {
    }

    private record PendingSelection(String documentId, String recommendedPropertyId) {
    }

    private record SelectionChoice(String propertyId) {
    }

    private record ConfirmedSelection(
        String documentId,
        String recommendedPropertyId,
        String confirmedPropertyId,
        java.time.Instant confirmedAt
    ) {
    }

    private static final class OriginalSelectionProjector
        implements AwaitCompletionProjector<PendingSelection, SelectionChoice, ConfirmedSelection> {
        @Override
        public ConfirmedSelection project(
            PendingSelection request,
            SelectionChoice completion,
            AwaitCompletionMetadata metadata
        ) {
            return new ConfirmedSelection(request.documentId(), request.recommendedPropertyId(),
                completion.propertyId(), metadata.completedAt());
        }
    }

    private static final class ChangedSelectionProjector
        implements AwaitCompletionProjector<PendingSelection, SelectionChoice, ConfirmedSelection> {
        @Override
        public ConfirmedSelection project(
            PendingSelection request,
            SelectionChoice completion,
            AwaitCompletionMetadata metadata
        ) {
            return new ConfirmedSelection(request.documentId(), request.recommendedPropertyId(),
                completion.propertyId(), metadata.completedAt());
        }
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

    private record FastCompletionAdapter(AwaitCoordinator coordinator) implements AwaitTransportAdapter<Object> {

        @Override
        public String type() {
            return "fast-completion";
        }

        @Override
        public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
            AwaitInteractionRecord interaction = request.interaction();
            AwaitCompletionCommand completion = new AwaitCompletionCommand(
                interaction.tenantId(),
                interaction.interactionId(),
                null,
                "completion-" + interaction.interactionId(),
                Map.of("status", "approved"),
                "fast-provider",
                System.currentTimeMillis());
            return coordinator.complete(completion)
                .replaceWith(new AwaitDispatchResult(Map.of("provider", "fast")));
        }
    }
}
