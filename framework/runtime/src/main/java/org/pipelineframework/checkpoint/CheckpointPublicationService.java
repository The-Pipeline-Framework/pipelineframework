package org.pipelineframework.checkpoint;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.annotation.PostConstruct;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/**
 * Publishes queue-async checkpoint outputs into configured runtime handoff targets.
 */
@ApplicationScoped
public class CheckpointPublicationService {

    private static final Logger LOG = Logger.getLogger(CheckpointPublicationService.class);

    @Inject
    Instance<CheckpointPublicationDescriptor> publicationDescriptors;

    @Inject
    Instance<CheckpointPublicationTargetDispatcher> targetDispatchers;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineHandoffConfig handoffConfig;

    private volatile CheckpointPublicationDescriptor descriptor;
    private volatile java.util.List<ResolvedCheckpointPublicationTarget> resolvedTargets = java.util.List.of();
    private volatile java.util.Map<PublicationTargetKind, CheckpointPublicationTargetDispatcher> dispatcherByKind =
        java.util.Map.of();

    /**
     * Initializes checkpoint publication support: selects the configured publication descriptor,
     * registers dispatchers by target kind, and resolves runtime publication targets.
     *
     * <p>If no publication descriptor is present this method leaves publication disabled and returns.</p>
     *
     * @throws IllegalStateException if the orchestrator mode is not QUEUE_ASYNC
     * @throws IllegalStateException if more than one dispatcher is registered for the same publication target kind
     * @throws IllegalStateException if the selected publication has no resolved runtime targets configured
     */
    @PostConstruct
    void initialize() {
        descriptor = publicationDescriptors.stream().findFirst().orElse(null);
        if (descriptor == null) {
            LOG.debug("No checkpoint publication descriptor resolved for this runtime");
            return;
        }
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            throw new IllegalStateException(
                "Checkpoint publication requires pipeline.orchestrator.mode=QUEUE_ASYNC");
        }
        java.util.Map<PublicationTargetKind, CheckpointPublicationTargetDispatcher> dispatchers =
            new java.util.EnumMap<>(PublicationTargetKind.class);
        targetDispatchers.stream().forEach(dispatcher -> {
            CheckpointPublicationTargetDispatcher duplicate = dispatchers.putIfAbsent(dispatcher.kind(), dispatcher);
            if (duplicate != null) {
                throw new IllegalStateException(
                    "Duplicate checkpoint publication dispatcher registered for kind " + dispatcher.kind());
            }
        });
        dispatcherByKind = java.util.Map.copyOf(dispatchers);
        resolvedTargets = resolveTargets(descriptor.publication());
        if (resolvedTargets.isEmpty()) {
            throw new IllegalStateException(
                "Checkpoint publication '" + descriptor.publication()
                    + "' requires at least one runtime binding under pipeline.handoff.bindings");
        }
        LOG.infof("Checkpoint publication enabled publication=%s targets=%d",
            descriptor.publication(), resolvedTargets.size());
    }

    /**
     * Publish a checkpoint to all configured runtime targets when a publication descriptor and payload are present.
     *
     * This method normalizes the provided payload, derives an idempotency key from the execution record and
     * normalized payload, constructs a checkpoint publication request, and dispatches it to every resolved target.
     * If publication is not configured, the payload is null, the normalized payload is null, or the orchestrator
     * mode is not QUEUE_ASYNC, publication is skipped or the returned Uni will fail as appropriate.
     *
     * @param record the execution record whose execution key, tenant, and execution id are used for idempotency and logging
     * @param resultPayload the raw result payload to be normalized and published
     * @return a Uni that completes with no value when all dispatches have completed; the Uni fails if publication cannot
     *         proceed due to an invalid orchestrator mode or if any target dispatch fails.
    public Uni<Void> publishIfConfigured(ExecutionRecord<Object, Object> record, Object resultPayload) {
        if (descriptor == null) {
            LOG.debug("Skipping checkpoint publication because no descriptor is configured");
            return Uni.createFrom().voidItem();
        }
        if (resultPayload == null) {
            LOG.warnf("Skipping checkpoint publication publication=%s execution=%s because result payload is null",
                descriptor.publication(),
                record == null ? "<unknown>" : record.executionId());
            return Uni.createFrom().voidItem();
        }
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Checkpoint publication requires pipeline.orchestrator.mode=QUEUE_ASYNC"));
        }

        Object normalizedPayload = descriptor.normalizePayload(resultPayload);
        if (normalizedPayload == null) {
            LOG.warnf("Skipping checkpoint publication publication=%s execution=%s because normalized payload is null",
                descriptor.publication(),
                record == null ? "<unknown>" : record.executionId());
            return Uni.createFrom().voidItem();
        }
        String idempotencyKey = CheckpointPublicationSupport.deriveIdempotencyKey(
            record.executionKey(),
            descriptor.idempotencyKeyFields(),
            normalizedPayload);
        CheckpointPublicationRequest request = new CheckpointPublicationRequest(
            descriptor.publication(),
            org.pipelineframework.config.pipeline.PipelineJson.mapper().valueToTree(normalizedPayload));
        return Uni.join().all(
            resolvedTargets.stream()
                .map(target -> dispatch(target, request, record, idempotencyKey))
                .toList()
        ).andCollectFailures().replaceWithVoid();
    }

    private Uni<Void> dispatch(
        ResolvedCheckpointPublicationTarget target,
        CheckpointPublicationRequest request,
        ExecutionRecord<Object, Object> record,
        String idempotencyKey
    ) {
        CheckpointPublicationTargetDispatcher dispatcher = dispatcherByKind.get(target.kind());
        if (dispatcher == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "No checkpoint publication dispatcher is available for target kind " + target.kind()));
        }
        LOG.infof("Publishing checkpoint publication=%s execution=%s target=%s kind=%s",
            request.publication(), record.executionId(), target.targetId(), target.kind());
        return dispatcher.dispatch(target, request, record.tenantId(), idempotencyKey);
    }

    private java.util.List<ResolvedCheckpointPublicationTarget> resolveTargets(String publication) {
        java.util.Map<String, PipelineHandoffConfig.PublicationBinding> bindings =
            handoffConfig == null || handoffConfig.bindings() == null ? java.util.Map.of() : handoffConfig.bindings();
        PipelineHandoffConfig.PublicationBinding binding = bindings.get(publication);
        if (binding == null || binding.targets() == null || binding.targets().isEmpty()) {
            return java.util.List.of();
        }
        java.util.Set<PublicationTargetKind> supportedKinds = dispatcherByKind.keySet();
        java.util.List<ResolvedCheckpointPublicationTarget> targets = new java.util.ArrayList<>();
        for (java.util.Map.Entry<String, PipelineHandoffConfig.TargetConfig> entry : binding.targets().entrySet()) {
            targets.add(resolveTarget(publication, entry.getKey(), entry.getValue(), supportedKinds));
        }
        return java.util.List.copyOf(targets);
    }

    private ResolvedCheckpointPublicationTarget resolveTarget(
        String publication,
        String targetId,
        PipelineHandoffConfig.TargetConfig target,
        java.util.Set<PublicationTargetKind> supportedKinds
    ) {
        if (targetId == null || targetId.isBlank()) {
            throw new IllegalStateException(
                "Checkpoint publication '" + publication + "' contains a target with a blank target id");
        }
        if (target == null || target.kind() == null) {
            throw new IllegalStateException(
                "Checkpoint publication '" + publication + "' target '" + targetId + "' must declare kind");
        }
        if (!supportedKinds.contains(target.kind())) {
            throw new IllegalStateException(
                "Checkpoint publication '" + publication + "' target '" + targetId
                    + "' uses unsupported kind " + target.kind());
        }
        return switch (target.kind()) {
            case GRPC -> resolveGrpcTarget(publication, targetId, target);
            case HTTP -> resolveHttpTarget(publication, targetId, target);
        };
    }

    private ResolvedCheckpointPublicationTarget resolveGrpcTarget(
        String publication,
        String targetId,
        PipelineHandoffConfig.TargetConfig target
    ) {
        String host = target.host()
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "Checkpoint publication '" + publication + "' target '" + targetId
                    + "' requires host for GRPC delivery"));
        int port = target.port()
            .filter(value -> value > 0)
            .orElseThrow(() -> new IllegalStateException(
                "Checkpoint publication '" + publication + "' target '" + targetId
                    + "' requires port for GRPC delivery"));
        return new ResolvedCheckpointPublicationTarget(
            publication,
            targetId,
            PublicationTargetKind.GRPC,
            PublicationEncoding.PROTO,
            null,
            null,
            host + ":" + port,
            target.plaintext() ? "PLAINTEXT" : "TLS");
    }

    private ResolvedCheckpointPublicationTarget resolveHttpTarget(
        String publication,
        String targetId,
        PipelineHandoffConfig.TargetConfig target
    ) {
        String baseUrl = target.baseUrl()
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "Checkpoint publication '" + publication + "' target '" + targetId
                    + "' requires base-url for HTTP delivery"));
        String path = target.path()
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElse(CheckpointPublicationResource.DEFAULT_PATH);
        String method = target.method() == null ? "POST" : target.method().trim().toUpperCase(java.util.Locale.ROOT);
        if (!"POST".equals(method)) {
            throw new IllegalStateException(
                "Checkpoint publication '" + publication + "' target '" + targetId
                    + "' only supports HTTP method POST");
        }
        PublicationEncoding encoding = target.encoding().orElse(PublicationEncoding.PROTO);
        String normalizedBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return new ResolvedCheckpointPublicationTarget(
            publication,
            targetId,
            PublicationTargetKind.HTTP,
            encoding,
            target.contentType().filter(value -> !value.isBlank()).orElse(
                encoding == PublicationEncoding.PROTO
                    ? org.pipelineframework.transport.http.ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF
                    : org.pipelineframework.transport.http.ProtobufHttpContentTypes.APPLICATION_JSON),
            target.idempotencyHeader().orElse("Idempotency-Key"),
            normalizedBase + normalizedPath,
            method);
    }
}
