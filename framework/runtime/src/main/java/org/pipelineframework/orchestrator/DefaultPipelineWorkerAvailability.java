package org.pipelineframework.orchestrator;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/**
 * Checks bundle availability on the worker selected by runtime configuration.
 */
@ApplicationScoped
public class DefaultPipelineWorkerAvailability implements PipelineWorkerAvailability {

    private static final Logger LOG = Logger.getLogger(DefaultPipelineWorkerAvailability.class);

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineBundleIdentityResolver identityResolver;

    @Inject
    RestPipelineTransitionWorker restWorker;

    @Inject
    GrpcPipelineTransitionWorker grpcWorker;

    @Override
    public Uni<PipelineWorkerAvailabilityResult> check(PipelineWorkerAvailabilityRequest request) {
        if (orchestratorConfig.workerRest().isEnabled()) {
            return restWorker.capabilities()
                .onItem().transform(capability -> match("rest", capability, request))
                .onFailure().recoverWithItem(failure ->
                    PipelineWorkerAvailabilityResult.unavailable(
                        "rest",
                        "REST transition worker capability check failed: " + failure.getMessage()));
        }
        if (orchestratorConfig.workerGrpc().isEnabled()) {
            return grpcWorker.capabilities()
                .onItem().transform(capability -> match("grpc", capability, request))
                .onFailure().recoverWithItem(failure ->
                    PipelineWorkerAvailabilityResult.unavailable(
                        "grpc",
                        "gRPC transition worker capability check failed: " + failure.getMessage()));
        }
        if (orchestratorConfig.workerSqs().isEnabled()) {
            return Uni.createFrom().item(() -> sqsAvailability(request));
        }
        return Uni.createFrom().item(() -> localAvailability(request));
    }

    private PipelineWorkerAvailabilityResult localAvailability(PipelineWorkerAvailabilityRequest request) {
        PipelineWorkerCapability capability = localCapability();
        return match("local", capability, request);
    }

    private PipelineWorkerAvailabilityResult sqsAvailability(PipelineWorkerAvailabilityRequest request) {
        var sqs = orchestratorConfig.workerSqs();
        if (sqs.pipelineId().filter(value -> !value.isBlank()).isEmpty()
            || sqs.contractVersion().filter(value -> !value.isBlank()).isEmpty()
            || sqs.releaseVersion().filter(value -> !value.isBlank()).isEmpty()
            || sqs.bundleVersionId().filter(value -> !value.isBlank()).isEmpty()) {
            return PipelineWorkerAvailabilityResult.unavailable(
                "sqs",
                "SQS worker bundle availability requires pipeline.orchestrator.worker.sqs.pipeline-id "
                    + "pipeline.orchestrator.worker.sqs.contract-version, "
                    + "pipeline.orchestrator.worker.sqs.release-version, "
                    + "and pipeline.orchestrator.worker.sqs.bundle-version-id");
        }
        PipelineWorkerCapability capability = new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            "sqs",
            sqs.pipelineId().orElseThrow().trim(),
            sqs.contractVersion().orElseThrow().trim(),
            sqs.releaseVersion().orElseThrow().trim(),
            sqs.bundleVersionId().orElseThrow().trim(),
            "",
            List.of(TransitionPayloadEncoding.JSON),
            List.of("sqs"));
        return match("sqs", capability, request);
    }

    private PipelineWorkerCapability localCapability() {
        PipelineBundleCapabilities capabilities = identityResolver.capabilities();
        return new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            "local",
            identityResolver.pipelineId(orchestratorConfig),
            identityResolver.contractVersion(),
            identityResolver.releaseVersion(orchestratorConfig),
            identityResolver.bundleVersionId(orchestratorConfig),
            identityResolver.bundleHash(),
            List.of(TransitionPayloadEncoding.JSON),
            capabilities.transitionWorkerProtocols());
    }

    private PipelineWorkerAvailabilityResult match(
        String providerName,
        PipelineWorkerCapability capability,
        PipelineWorkerAvailabilityRequest request) {
        if (!PipelineWorkerCapability.PROTOCOL_VERSION.equals(capability.protocolVersion())) {
            return PipelineWorkerAvailabilityResult.unavailable(
                providerName,
                "Worker capability protocol version is unsupported");
        }
        if (!request.pipelineId().equals(capability.pipelineId())
            || !request.contractVersion().equals(capability.contractVersion())
            || !request.releaseVersion().equals(capability.releaseVersion())
            || !request.bundleVersionId().equals(capability.bundleVersionId())) {
            return PipelineWorkerAvailabilityResult.unavailable(
                providerName,
                "Worker hosts pipelineId=" + capability.pipelineId()
                    + ", contractVersion=" + capability.contractVersion()
                    + ", releaseVersion=" + capability.releaseVersion()
                    + ", bundleVersionId=" + capability.bundleVersionId()
                    + " but active release is pipelineId=" + request.pipelineId()
                    + ", contractVersion=" + request.contractVersion()
                    + ", releaseVersion=" + request.releaseVersion()
                    + ", bundleVersionId=" + request.bundleVersionId());
        }
        LOG.debugf(
            "Selected %s worker is available for tenant=%s pipelineId=%s releaseVersion=%s bundleVersionId=%s",
            providerName,
            request.tenantId(),
            request.pipelineId(),
            request.releaseVersion(),
            request.bundleVersionId());
        return PipelineWorkerAvailabilityResult.available(providerName, capability);
    }
}
