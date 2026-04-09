package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

class CheckpointPublicationServiceTest {

    @Test
    void initializeFailsWhenPublicationHasNoRuntimeBindings() {
        CheckpointPublicationService service = new CheckpointPublicationService();
        service.publicationDescriptors = descriptors(new TestDescriptor("orders-ready", List.of("orderId")));
        service.targetDispatchers = dispatchers();
        service.orchestratorConfig = orchestratorConfig();
        service.handoffConfig = handoffConfig(Map.of());

        IllegalStateException error = assertThrows(IllegalStateException.class, service::initialize);
        assertEquals(
            "Checkpoint publication 'orders-ready' requires at least one runtime binding under pipeline.handoff.bindings",
            error.getMessage());
    }

    @Test
    void publishFansOutToConfiguredTargets() {
        CheckpointPublicationService service = new CheckpointPublicationService();
        service.publicationDescriptors = descriptors(new TestDescriptor("orders-ready", List.of("orderId")));
        TestDispatcher dispatcher = new TestDispatcher();
        service.targetDispatchers = dispatchers(dispatcher);
        service.orchestratorConfig = orchestratorConfig();
        service.handoffConfig = handoffConfig(Map.of(
            "orders-ready",
            publicationBinding(Map.of(
                "deliver", httpTarget("http://localhost:8081", "/pipeline/checkpoints/publish"),
                "audit", httpTarget("http://localhost:8082", "/pipeline/checkpoints/publish")))));
        service.initialize();

        service.publishIfConfigured(record(), new PublishedOrder("o-1", "c-1")).await().indefinitely();

        assertEquals(2, dispatcher.targetIds.size());
        assertTrue(dispatcher.targetIds.containsAll(List.of("deliver", "audit")));
        assertEquals(List.of("o-1", "o-1"), dispatcher.idempotencyKeys);
    }

    @SuppressWarnings("unchecked")
    private Instance<CheckpointPublicationDescriptor> descriptors(CheckpointPublicationDescriptor... descriptors) {
        Instance<CheckpointPublicationDescriptor> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(descriptors));
        return instance;
    }

    @SuppressWarnings("unchecked")
    private Instance<CheckpointPublicationTargetDispatcher> dispatchers(CheckpointPublicationTargetDispatcher... dispatchers) {
        Instance<CheckpointPublicationTargetDispatcher> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(dispatchers));
        return instance;
    }

    private PipelineOrchestratorConfig orchestratorConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        return config;
    }

    private PipelineHandoffConfig handoffConfig(Map<String, PipelineHandoffConfig.PublicationBinding> bindings) {
        PipelineHandoffConfig config = mock(PipelineHandoffConfig.class);
        when(config.bindings()).thenReturn(bindings);
        return config;
    }

    private PipelineHandoffConfig.PublicationBinding publicationBinding(Map<String, PipelineHandoffConfig.TargetConfig> targets) {
        PipelineHandoffConfig.PublicationBinding binding = mock(PipelineHandoffConfig.PublicationBinding.class);
        when(binding.targets()).thenReturn(targets);
        return binding;
    }

    private PipelineHandoffConfig.TargetConfig httpTarget(String baseUrl, String path) {
        PipelineHandoffConfig.TargetConfig target = mock(PipelineHandoffConfig.TargetConfig.class);
        when(target.kind()).thenReturn(PublicationTargetKind.HTTP);
        when(target.baseUrl()).thenReturn(java.util.Optional.of(baseUrl));
        when(target.path()).thenReturn(java.util.Optional.of(path));
        when(target.method()).thenReturn("POST");
        when(target.encoding()).thenReturn(java.util.Optional.of(PublicationEncoding.PROTO));
        when(target.contentType()).thenReturn(
            java.util.Optional.of(org.pipelineframework.transport.http.ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF));
        when(target.idempotencyHeader()).thenReturn(java.util.Optional.empty());
        return target;
    }

    private ExecutionRecord<Object, Object> record() {
        return new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            null,
            ExecutionStatus.SUCCEEDED,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            null,
            null,
            null,
            null,
            1L,
            1L,
            1L);
    }

    private record TestDescriptor(String publication, List<String> idempotencyKeyFields)
        implements CheckpointPublicationDescriptor {
    }

    private record PublishedOrder(String orderId, String customerId) {
    }

    private static final class TestDispatcher implements CheckpointPublicationTargetDispatcher {
        private final java.util.List<String> targetIds = new java.util.ArrayList<>();
        private final java.util.List<String> idempotencyKeys = new java.util.ArrayList<>();

        @Override
        public PublicationTargetKind kind() {
            return PublicationTargetKind.HTTP;
        }

        @Override
        public Uni<Void> dispatch(
            ResolvedCheckpointPublicationTarget target,
            CheckpointPublicationRequest request,
            String tenantId,
            String idempotencyKey
        ) {
            targetIds.add(target.targetId());
            idempotencyKeys.add(idempotencyKey);
            return Uni.createFrom().voidItem();
        }
    }
}
