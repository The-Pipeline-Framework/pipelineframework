package org.pipelineframework.orchestrator;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Selects the transition worker from configured remote targets.
 */
@ApplicationScoped
public class PipelineTransitionWorkerSelector {

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    RestPipelineTransitionWorker restWorker;

    @Inject
    GrpcPipelineTransitionWorker grpcWorker;

    @Inject
    SqsPipelineTransitionWorker sqsWorker;

    @PostConstruct
    void validateConfiguredTargets() {
        boolean restEnabled = orchestratorConfig.workerRest().isEnabled();
        boolean grpcEnabled = orchestratorConfig.workerGrpc().isEnabled();
        boolean sqsEnabled = orchestratorConfig.workerSqs().isEnabled();
        int remoteTargets = remoteTargetCount(restEnabled, grpcEnabled, sqsEnabled);
        validateRemoteWorkerRequirement(remoteTargets);
        validateStartup(restEnabled, grpcEnabled, sqsEnabled);
    }

    /**
     * Selects a remote worker when exactly one remote target is configured; otherwise uses the local worker.
     *
     * @param localWorker local in-process worker
     * @return selected worker
     */
    public PipelineTransitionWorker select(PipelineTransitionWorker localWorker) {
        boolean restEnabled = orchestratorConfig.workerRest().isEnabled();
        boolean grpcEnabled = orchestratorConfig.workerGrpc().isEnabled();
        boolean sqsEnabled = orchestratorConfig.workerSqs().isEnabled();
        int remoteTargets = remoteTargetCount(restEnabled, grpcEnabled, sqsEnabled);
        if (remoteTargets == 0) {
            return localWorker;
        }
        if (restEnabled) {
            return restWorker;
        }
        if (grpcEnabled) {
            return grpcWorker;
        }
        return sqsWorker;
    }

    private int remoteTargetCount(boolean restEnabled, boolean grpcEnabled, boolean sqsEnabled) {
        int remoteTargets = (restEnabled ? 1 : 0) + (grpcEnabled ? 1 : 0) + (sqsEnabled ? 1 : 0);
        if (remoteTargets > 1) {
            throw new IllegalStateException("Ambiguous transition worker target: configure only one of "
                + "pipeline.orchestrator.worker.rest.base-url, pipeline.orchestrator.worker.grpc.endpoint, "
                + "or pipeline.orchestrator.worker.sqs.request-queue-url");
        }
        return remoteTargets;
    }

    private void validateStartup(boolean restEnabled, boolean grpcEnabled, boolean sqsEnabled) {
        validateIfEnabled(restEnabled, restWorker);
        validateIfEnabled(grpcEnabled, grpcWorker);
        validateIfEnabled(sqsEnabled, sqsWorker);
    }

    private void validateRemoteWorkerRequirement(int remoteTargets) {
        PipelineOrchestratorConfig.ControlPlaneConfig controlPlane = orchestratorConfig.controlPlane();
        if (controlPlane == null || !controlPlane.enabled() || !controlPlane.requireRemoteWorker()) {
            return;
        }
        if (remoteTargets == 0) {
            throw new IllegalStateException("pipeline.orchestrator.control-plane.require-remote-worker=true requires "
                + "one configured transition worker target: pipeline.orchestrator.worker.rest.base-url, "
                + "pipeline.orchestrator.worker.grpc.endpoint, or pipeline.orchestrator.worker.sqs.request-queue-url");
        }
    }

    private void validateIfEnabled(boolean enabled, PipelineTransitionWorker worker) {
        if (!enabled) {
            return;
        }
        worker.startupValidationError(orchestratorConfig).ifPresent(message -> {
            throw new IllegalStateException(message);
        });
    }
}
