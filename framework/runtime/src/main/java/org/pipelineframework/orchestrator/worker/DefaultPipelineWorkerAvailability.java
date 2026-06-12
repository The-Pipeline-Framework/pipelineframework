package org.pipelineframework.orchestrator.worker;

import java.util.List;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.GrpcPipelineTransitionWorker;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseRuntimeBeans;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.RestPipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionPayloadEncoding;

/**
 * Checks release availability on the worker selected by runtime configuration.
 */
@ApplicationScoped
public class DefaultPipelineWorkerAvailability implements PipelineWorkerAvailability {

    private static final Logger LOG = Logger.getLogger(DefaultPipelineWorkerAvailability.class);

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineReleaseIdentityResolver identityResolver;

    @Inject
    RestPipelineTransitionWorker restWorker;

    @Inject
    GrpcPipelineTransitionWorker grpcWorker;

    @Inject
    PipelineWorkerRegistry workerRegistry;

    private volatile PipelineWorkerRegistry fallbackWorkerRegistry;

    @Override
    public Uni<PipelineWorkerAvailabilityResult> check(PipelineWorkerAvailabilityRequest request) {
        if (orchestratorConfig.workerRest().isEnabled()) {
            return restWorker.capabilities()
                .onItem().transformToUni(capability -> matchWithLifecycle("rest", capability, request))
                .onFailure().recoverWithItem(failure ->
                    PipelineWorkerAvailabilityResult.unavailable(
                        "rest",
                        "REST transition worker capability check failed: " + failure.getMessage()));
        }
        if (orchestratorConfig.workerGrpc().isEnabled()) {
            return grpcWorker.capabilities()
                .onItem().transformToUni(capability -> matchWithLifecycle("grpc", capability, request))
                .onFailure().recoverWithItem(failure ->
                    PipelineWorkerAvailabilityResult.unavailable(
                        "grpc",
                        "gRPC transition worker capability check failed: " + failure.getMessage()));
        }
        if (orchestratorConfig.workerSqs().isEnabled()) {
            return sqsAvailability(request);
        }
        return localAvailability(request);
    }

    private Uni<PipelineWorkerAvailabilityResult> localAvailability(PipelineWorkerAvailabilityRequest request) {
        PipelineWorkerCapability capability = localCapability();
        return matchWithLifecycle("local", capability, request);
    }

    private Uni<PipelineWorkerAvailabilityResult> sqsAvailability(PipelineWorkerAvailabilityRequest request) {
        var sqs = orchestratorConfig.workerSqs();
        if (sqs.pipelineId().filter(value -> !value.isBlank()).isEmpty()
            || sqs.contractVersion().filter(value -> !value.isBlank()).isEmpty()
            || sqs.releaseVersion().filter(value -> !value.isBlank()).isEmpty()) {
            return Uni.createFrom().item(PipelineWorkerAvailabilityResult.unavailable(
                "sqs",
                "SQS worker release availability requires pipeline.orchestrator.worker.sqs.pipeline-id "
                    + "pipeline.orchestrator.worker.sqs.contract-version, "
                    + "and pipeline.orchestrator.worker.sqs.release-version"));
        }
        PipelineWorkerCapability capability = new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            "sqs",
            sqs.pipelineId().orElseThrow().trim(),
            sqs.contractVersion().orElseThrow().trim(),
            sqs.releaseVersion().orElseThrow().trim(),
            sqs.artifactId().orElse("").trim(),
            sqs.artifactDigest().orElse("").trim(),
            List.of(TransitionPayloadEncoding.JSON),
            List.of("sqs"));
        return matchWithLifecycle("sqs", capability, request);
    }

    private PipelineWorkerCapability localCapability() {
        PipelineBundleCapabilities capabilities = identityResolver.capabilities();
        return new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            "local",
            identityResolver.pipelineId(orchestratorConfig),
            identityResolver.contractVersion(),
            identityResolver.releaseVersion(orchestratorConfig),
            identityResolver.artifactId(orchestratorConfig),
            identityResolver.artifactDigest(orchestratorConfig),
            List.of(TransitionPayloadEncoding.JSON),
            capabilities.transitionWorkerProtocols());
    }

    private Uni<PipelineWorkerAvailabilityResult> matchWithLifecycle(
        String providerName,
        PipelineWorkerCapability capability,
        PipelineWorkerAvailabilityRequest request) {
        PipelineWorkerAvailabilityResult capabilityResult = match(providerName, capability, request);
        if (!capabilityResult.available()) {
            return Uni.createFrom().item(capabilityResult);
        }
        long now = System.currentTimeMillis();
        return workerRegistry().matching(
                request,
                providerName,
                now,
                PipelineReleaseRuntimeBeans.workerStaleAfter(orchestratorConfig))
            .onItem().transform(records -> lifecycleResult(providerName, capability, request, records));
    }

    private PipelineWorkerAvailabilityResult lifecycleResult(
        String providerName,
        PipelineWorkerCapability capability,
        PipelineWorkerAvailabilityRequest request,
        List<PipelineWorkerRecord> records) {
        boolean hasMatchingHealthy = records.stream()
            .anyMatch(record -> record.state() == PipelineWorkerState.HEALTHY
                && record.matches(request, providerName));
        if (hasMatchingHealthy) {
            LOG.debugf(
                "Selected %s worker is healthy for tenant=%s pipelineId=%s releaseVersion=%s",
                providerName,
                request.tenantId(),
                request.pipelineId(),
                request.releaseVersion());
            return PipelineWorkerAvailabilityResult.available(providerName, capability);
        }
        boolean hasArtifactMismatch = records.stream()
            .anyMatch(record -> record.hasArtifactMismatch(request, providerName));
        if (hasArtifactMismatch) {
            return PipelineWorkerAvailabilityResult.unavailable(
                providerName,
                "Registered worker artifact identity does not match active release");
        }
        if (!records.isEmpty()) {
            String states = records.stream()
                .map(record -> record.workerId() + "=" + record.state())
                .sorted()
                .collect(Collectors.joining(", "));
            return PipelineWorkerAvailabilityResult.unavailable(
                providerName,
                "No healthy " + providerName + " worker lifecycle record for active release; current states: "
                    + states);
        }
        return PipelineWorkerAvailabilityResult.unavailable(
            providerName,
            "No worker lifecycle record for active release");
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
            || !request.releaseVersion().equals(capability.releaseVersion())) {
            return PipelineWorkerAvailabilityResult.unavailable(
                providerName,
                "Worker hosts pipelineId=" + capability.pipelineId()
                    + ", contractVersion=" + capability.contractVersion()
                    + ", releaseVersion=" + capability.releaseVersion()
                    + " but active release is pipelineId=" + request.pipelineId()
                    + ", contractVersion=" + request.contractVersion()
                    + ", releaseVersion=" + request.releaseVersion());
        }
        if (!request.artifactDigest().isBlank()
            && !capability.artifactDigest().isBlank()
            && !request.artifactDigest().equals(capability.artifactDigest())) {
            return PipelineWorkerAvailabilityResult.unavailable(
                providerName,
                "Worker artifact digest " + capability.artifactDigest()
                    + " does not match active release artifact digest " + request.artifactDigest());
        }
        if (!request.artifactId().isBlank()
            && !capability.artifactId().isBlank()
            && !request.artifactId().equals(capability.artifactId())) {
            return PipelineWorkerAvailabilityResult.unavailable(
                providerName,
                "Worker artifact id " + capability.artifactId()
                    + " does not match active release artifact id " + request.artifactId());
        }
        return PipelineWorkerAvailabilityResult.available(providerName, capability);
    }

    private PipelineWorkerRegistry workerRegistry() {
        if (workerRegistry != null) {
            return workerRegistry;
        }
        PipelineWorkerRegistry fallback = fallbackWorkerRegistry;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackWorkerRegistry;
                if (fallback == null) {
                    fallback = new InMemoryPipelineWorkerRegistry();
                    fallbackWorkerRegistry = fallback;
                }
            }
        }
        return fallback;
    }
}
