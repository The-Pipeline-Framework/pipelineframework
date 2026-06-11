package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineTransitionWorkerSelectorTest {

    private PipelineTransitionWorkerSelector selector;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.RestWorkerConfig restConfig;
    private PipelineOrchestratorConfig.GrpcWorkerConfig grpcConfig;
    private PipelineOrchestratorConfig.SqsWorkerConfig sqsConfig;
    private RestPipelineTransitionWorker restWorker;
    private GrpcPipelineTransitionWorker grpcWorker;
    private SqsPipelineTransitionWorker sqsWorker;
    private PipelineTransitionWorker localWorker;

    @BeforeEach
    void setUp() {
        selector = new PipelineTransitionWorkerSelector();
        config = mock(PipelineOrchestratorConfig.class);
        restConfig = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        grpcConfig = mock(PipelineOrchestratorConfig.GrpcWorkerConfig.class);
        sqsConfig = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        restWorker = mock(RestPipelineTransitionWorker.class);
        grpcWorker = mock(GrpcPipelineTransitionWorker.class);
        sqsWorker = mock(SqsPipelineTransitionWorker.class);
        localWorker = command -> Uni.createFrom().item(TransitionResultEnvelope.completed(
            new JsonTransitionPayloadCodec(),
            java.util.List.of()));
        selector.orchestratorConfig = config;
        selector.restWorker = restWorker;
        selector.grpcWorker = grpcWorker;
        selector.sqsWorker = sqsWorker;
        when(config.workerRest()).thenReturn(restConfig);
        when(config.workerGrpc()).thenReturn(grpcConfig);
        when(config.workerSqs()).thenReturn(sqsConfig);
    }

    @Test
    void selectsLocalWorkerWhenNoRemoteTargetIsConfigured() {
        when(restConfig.isEnabled()).thenReturn(false);
        when(grpcConfig.isEnabled()).thenReturn(false);
        when(sqsConfig.isEnabled()).thenReturn(false);

        PipelineTransitionWorker selected = selector.select(localWorker);

        assertSame(localWorker, selected);
    }

    @Test
    void selectsRestWorkerWhenRestBaseUrlIsConfigured() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(grpcConfig.isEnabled()).thenReturn(false);
        when(sqsConfig.isEnabled()).thenReturn(false);

        PipelineTransitionWorker selected = selector.select(localWorker);

        assertSame(restWorker, selected);
    }

    @Test
    void selectsGrpcWorkerWhenGrpcEndpointIsConfigured() {
        when(restConfig.isEnabled()).thenReturn(false);
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(sqsConfig.isEnabled()).thenReturn(false);

        PipelineTransitionWorker selected = selector.select(localWorker);

        assertSame(grpcWorker, selected);
    }

    @Test
    void selectsSqsWorkerWhenSqsRequestQueueIsConfigured() {
        when(restConfig.isEnabled()).thenReturn(false);
        when(grpcConfig.isEnabled()).thenReturn(false);
        when(sqsConfig.isEnabled()).thenReturn(true);

        PipelineTransitionWorker selected = selector.select(localWorker);

        assertSame(sqsWorker, selected);
    }

    @Test
    void rejectsAmbiguousRemoteWorkerTargets() {
        assertAmbiguousSelection(true, true, false);
        assertAmbiguousSelection(true, false, true);
        assertAmbiguousSelection(false, true, true);
        assertAmbiguousSelection(true, true, true);
    }

    private void assertAmbiguousSelection(boolean restEnabled, boolean grpcEnabled, boolean sqsEnabled) {
        when(restConfig.isEnabled()).thenReturn(restEnabled);
        when(grpcConfig.isEnabled()).thenReturn(grpcEnabled);
        when(sqsConfig.isEnabled()).thenReturn(sqsEnabled);

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> selector.select(localWorker));

        assertTrue(error.getMessage().contains("Ambiguous transition worker target"));
    }
}
