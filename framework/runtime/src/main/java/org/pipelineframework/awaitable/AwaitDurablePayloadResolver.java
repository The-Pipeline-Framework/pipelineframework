package org.pipelineframework.awaitable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.pipelineframework.orchestrator.CanonicalPayloadBinding;
import org.pipelineframework.orchestrator.BoundedExpiringCache;
import org.pipelineframework.orchestrator.CompiledDurablePayloadPlan;
import org.pipelineframework.orchestrator.DurablePayloadPlanRegistry;
import org.pipelineframework.orchestrator.DurablePayloadReleaseCoordinate;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.CanonicalPayloadBindingLookup;
import org.pipelineframework.orchestrator.CanonicalPayloadRuntimeClassLoader;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Resolves pinned-release plans for the canonical request/response fields of await interactions. */
@ApplicationScoped
public class AwaitDurablePayloadResolver {
    public enum Slot { REQUEST, RESPONSE }

    /** Direct override for focused tests; production selection is provider-name based. */
    ExecutionStateStore executionStateStore;
    @Inject Instance<ExecutionStateStore> executionStateStores;
    @Inject PipelineOrchestratorConfig orchestratorConfig;
    @Inject PipelineReleaseRegistry releaseRegistry;
    @Inject AwaitStepDescriptorFactory descriptors;
    @Inject JsonDurablePayloadCodec codec;

    private final DurablePayloadPlanRegistry plans = new DurablePayloadPlanRegistry();
    private static final int RELEASE_CACHE_MAXIMUM_SIZE = 256;
    private static final Duration RELEASE_CACHE_EXPIRY = Duration.ofMinutes(15);
    private static final int EXECUTION_CACHE_MAXIMUM_SIZE = 4_096;
    private final BoundedExpiringCache<ReleaseCacheKey, PipelineReleaseRecord> releases =
        new BoundedExpiringCache<>(RELEASE_CACHE_MAXIMUM_SIZE, RELEASE_CACHE_EXPIRY);
    private final BoundedExpiringCache<ExecutionCacheKey, PinnedExecution> executions =
        new BoundedExpiringCache<>(EXECUTION_CACHE_MAXIMUM_SIZE, RELEASE_CACHE_EXPIRY);
    private static final Duration RELEASE_LOOKUP_TIMEOUT = Duration.ofSeconds(10);

    public String encode(AwaitInteractionRecord interaction, Slot slot, Object value) {
        CompiledDurablePayloadPlan plan = resolve(interaction, slot);
        try {
            return PipelineJson.mapper().writeValueAsString(codec.encode(value, plan));
        } catch (Exception e) {
            throw failure(interaction, slot, plan.binding().canonicalTypeId(), "encode", e);
        }
    }

    /** Returns whether the interaction's pinned release has the v3 canonical binding catalog. */
    public boolean supportsTypedPayloads(AwaitInteractionRecord interaction) {
        return findOwningExecution(interaction)
            .map(execution -> pinnedRelease(interaction.tenantId(), execution).contract().schemaVersion() >= 2)
            // Remote transition workers do not own coordinator execution state. They return a
            // transport snapshot to the coordinator, which owns durable payload persistence.
            .orElse(false);
    }

    /**
     * Resolves and caches the immutable release metadata before a concurrent itemized await dispatch
     * starts. This keeps the release-registry lookup out of the per-item Dynamo worker pool.
     */
    public Uni<Void> preload(String tenantId, String executionId) {
        ExecutionCacheKey key = new ExecutionCacheKey(tenantId, executionId);
        return executionStateStore().getExecution(tenantId, executionId)
            .onItem().transformToUni(optional -> {
                if (optional.isEmpty()) {
                    return Uni.createFrom().voidItem();
                }
                return preloadRelease(key, PinnedExecution.from(optional.get()), tenantId).replaceWithVoid();
            });
    }

    public Object decode(AwaitInteractionRecord interaction, Slot slot, String stored) {
        try {
            TypedDurablePayload payload = TypedDurablePayload.fromSerializedBytes(stored.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(() -> new IllegalArgumentException("not a typed durable payload"));
            return decodeEnvelope(interaction, slot, payload);
        } catch (Exception e) {
            throw failure(interaction, slot, slot.name().toLowerCase(), "decode", e);
        }
    }

    /** Restores a typed envelope that crossed a transition command as JSON object metadata. */
    public Object decodeEnvelope(AwaitInteractionRecord interaction, Slot slot, TypedDurablePayload payload) {
        try {
            CompiledDurablePayloadPlan plan = resolve(interaction, slot);
            return codec.decode(payload, plan);
        } catch (Exception e) {
            throw failure(interaction, slot, slot.name().toLowerCase(), "decode", e);
        }
    }

    /**
     * Decode-only compatibility for the former class-wrapper JSON shape. The stored class hint is
     * deliberately ignored: the pinned release and expected slot choose the only permissible target.
     */
    public Object decodeLegacy(AwaitInteractionRecord interaction, Slot slot, String stored) {
        CompiledDurablePayloadPlan plan = resolve(interaction, slot);
        try {
            Object decoded = PipelineJson.mapper().readValue(stored, Object.class);
            Object body = decoded instanceof Map<?, ?> map && map.containsKey("_tpf_payload")
                ? map.get("_tpf_payload")
                : decoded;
            byte[] canonicalBytes = PipelineJson.mapper().writeValueAsBytes(body);
            return codec.decode(new TypedDurablePayload(
                plan.binding().canonicalTypeId(),
                plan.binding().typeExpressionFingerprint(),
                plan.binding().catalogFingerprint(),
                JsonDurablePayloadCodec.ENCODING,
                JsonDurablePayloadCodec.ENCODING_VERSION,
                canonicalBytes), plan);
        } catch (Exception e) {
            throw failure(interaction, slot, plan.binding().canonicalTypeId(), "legacy decode", e);
        }
    }

    private CompiledDurablePayloadPlan resolve(AwaitInteractionRecord interaction, Slot slot) {
        PinnedExecution execution = owningExecution(interaction);
        PipelineReleaseRecord release = pinnedRelease(interaction.tenantId(), execution);
        AwaitStepDescriptor descriptor = descriptors.descriptorByStepIdNow(interaction.stepId());
        String requestedTypeId = slot == Slot.REQUEST ? descriptor.inputType() : interaction.outputType();
        var resolvedDefinition = CanonicalPayloadBindingLookup.resolve(
            release.contract().canonicalTypes(), requestedTypeId);
        String canonicalTypeId = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
            .orElse(requestedTypeId);
        Map<String, Object> canonicalDefinition = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::definition)
            .orElse(Map.of());
        String runtimeType;
        String expression;
        String catalog;
        if (resolvedDefinition.isPresent()) {
            runtimeType = requiredString(canonicalDefinition, "runtimeClass", canonicalTypeId);
            expression = requiredString(canonicalDefinition, "definitionFingerprint", canonicalTypeId);
            catalog = release.contract().canonicalCatalogFingerprint();
            if (catalog.isBlank()) {
                throw new IllegalStateException("pinned release canonical catalog fingerprint is unavailable");
            }
        } else if (release.contract().schemaVersion() == 1) {
            runtimeType = slot == Slot.REQUEST ? descriptor.inputType() : interaction.outputType();
            expression = fingerprint(canonicalTypeId);
            catalog = release.contract().contractHash();
        } else {
            throw new IllegalStateException("pinned release has no canonical binding for " + canonicalTypeId);
        }
        if (slot == Slot.RESPONSE) {
            String descriptorOutputType = CanonicalPayloadBindingLookup.resolve(
                release.contract().canonicalTypes(), descriptor.outputType())
                .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
                .orElse(descriptor.outputType());
            if (!descriptorOutputType.equals(canonicalTypeId)) {
                throw new IllegalStateException("await durable-contract compatibility failed for stepId="
                    + interaction.stepId() + ": durable canonical outputType=" + canonicalTypeId
                    + " differs from rebuilt descriptor outputType=" + descriptorOutputType);
            }
        }
        DurablePayloadReleaseCoordinate coordinate = execution.coordinate();
        var cached = plans.find(coordinate, expression);
        if (cached.isPresent()) {
            return cached.get();
        }
        Class<?> runtimeClass;
        try {
            runtimeClass = CanonicalPayloadRuntimeClassLoader.load(runtimeType, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("pinned release binding class is unavailable: " + runtimeType, e);
        }
        CanonicalPayloadBinding binding = new CanonicalPayloadBinding(canonicalTypeId, expression,
            catalog, runtimeClass);
        plans.activate(coordinate, Map.of(expression, binding));
        return plans.plan(coordinate, expression);
    }

    private PipelineReleaseRecord pinnedRelease(String tenantId, PinnedExecution execution) {
        DurablePayloadReleaseCoordinate coordinate = execution.coordinate();
        return releases.getOrLoad(new ReleaseCacheKey(tenantId, coordinate), ignored ->
            releaseRegistry.get(tenantId, execution.pipelineId(), execution.releaseVersion())
                .await().atMost(RELEASE_LOOKUP_TIMEOUT)
                .orElseThrow(() -> new IllegalStateException("pinned release is unavailable")));
    }

    private Uni<PipelineReleaseRecord> preloadRelease(
        ExecutionCacheKey executionKey,
        PinnedExecution execution,
        String tenantId
    ) {
        executions.getOrLoad(executionKey, ignored -> execution);
        DurablePayloadReleaseCoordinate coordinate = execution.coordinate();
        return releaseRegistry.get(tenantId, execution.pipelineId(), execution.releaseVersion())
            .onItem().transform(optional -> optional
                .orElseThrow(() -> new OwningExecutionUnavailableException()))
            .onItem().invoke(release -> releases.getOrLoad(
                new ReleaseCacheKey(tenantId, coordinate), ignored -> release));
    }

    private PinnedExecution owningExecution(AwaitInteractionRecord interaction) {
        return findOwningExecution(interaction)
            .orElseThrow(() -> new IllegalStateException("owning execution is unavailable"));
    }

    private java.util.Optional<PinnedExecution> findOwningExecution(AwaitInteractionRecord interaction) {
        ExecutionCacheKey key = new ExecutionCacheKey(interaction.tenantId(), interaction.executionId());
        try {
            return java.util.Optional.of(executions.getOrLoad(key, ignored -> executionStateStore()
                .getExecution(interaction.tenantId(), interaction.executionId())
                .await().atMost(RELEASE_LOOKUP_TIMEOUT)
                .map(PinnedExecution::from)
                .orElseThrow(() -> new OwningExecutionUnavailableException())));
        } catch (OwningExecutionUnavailableException ignored) {
            return java.util.Optional.empty();
        }
    }

    private record ReleaseCacheKey(String tenantId, DurablePayloadReleaseCoordinate coordinate) {
    }

    private record ExecutionCacheKey(String tenantId, String executionId) {
    }

    private record PinnedExecution(String pipelineId, String contractVersion, String releaseVersion) {
        private static PinnedExecution from(ExecutionRecord<?, ?> execution) {
            return new PinnedExecution(execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        }

        private DurablePayloadReleaseCoordinate coordinate() {
            return new DurablePayloadReleaseCoordinate(pipelineId, contractVersion, releaseVersion);
        }
    }

    private static final class OwningExecutionUnavailableException extends RuntimeException {
    }

    private static IllegalStateException failure(AwaitInteractionRecord interaction, Slot slot, String type, String action, Exception cause) {
        return new IllegalStateException("Await durable payload " + action + " failed: interactionId="
            + interaction.interactionId() + ", executionId=" + interaction.executionId() + ", slot=" + slot
            + ", canonicalTypeId=" + type, cause);
    }

    private ExecutionStateStore executionStateStore() {
        if (executionStateStore != null) {
            return executionStateStore;
        }
        if (executionStateStores == null || orchestratorConfig == null) {
            throw new IllegalStateException("Await durable payload resolver has no execution-state provider");
        }
        String requested = orchestratorConfig.stateProvider();
        return executionStateStores.stream()
            .filter(store -> requested == null || requested.isBlank() || requested.equalsIgnoreCase(store.providerName()))
            .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No ExecutionStateStore provider found for '" + requested + "'"));
    }

    private static String fingerprint(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to fingerprint canonical type expression", e);
        }
    }

    private static String requiredString(Map<String, Object> definition, String field, String canonicalTypeId) {
        Object value = definition.get(field);
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalStateException("pinned release canonical binding " + canonicalTypeId
                + " does not declare " + field);
        }
        return string;
    }
}
