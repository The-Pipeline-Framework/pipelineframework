package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;

/** Resolves cached canonical payload plans from the release pinned by an execution record. */
@ApplicationScoped
public class ExecutionDurablePayloadResolver {
    public enum Slot { INPUT, CONTINUATION_INPUT, RESULT }

    @Inject PipelineReleaseRegistry releaseRegistry;
    @Inject JsonDurablePayloadCodec codec;

    private final DurablePayloadPlanRegistry plans = new DurablePayloadPlanRegistry();
    private static final int RELEASE_CACHE_MAXIMUM_SIZE = 256;
    private static final Duration RELEASE_CACHE_EXPIRY = Duration.ofMinutes(15);
    private final BoundedExpiringCache<ReleaseCacheKey, PipelineReleaseRecord> releases =
        new BoundedExpiringCache<>(RELEASE_CACHE_MAXIMUM_SIZE, RELEASE_CACHE_EXPIRY);
    private static final Duration RELEASE_LOOKUP_TIMEOUT = Duration.ofSeconds(10);

    public String encode(ExecutionRecord<?, ?> execution, Slot slot, Object value) {
        CompiledDurablePayloadPlan plan = resolve(execution, slot, value instanceof List<?>);
        return encode(execution, slot, value, plan);
    }

    /** Encodes an explicitly declared boundary type through the execution's pinned release. */
    public String encode(ExecutionRecord<?, ?> execution, String canonicalTypeId, Object value) {
        CompiledDurablePayloadPlan plan = resolve(
            pinnedRelease(execution), execution, canonicalTypeId, value instanceof List<?>);
        return encode(execution, Slot.CONTINUATION_INPUT, value, plan);
    }

    private String encode(
        ExecutionRecord<?, ?> execution,
        Slot slot,
        Object value,
        CompiledDurablePayloadPlan plan
    ) {
        try {
            return PipelineJson.mapper().writeValueAsString(codec.encode(value, plan));
        } catch (Exception error) {
            throw failure(execution, slot, plan.binding().canonicalTypeId(), "encode", error);
        }
    }

    public Object decode(ExecutionRecord<?, ?> execution, Slot slot, String stored) {
        try {
            TypedDurablePayload payload = TypedDurablePayload.fromSerializedBytes(stored.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(() -> new IllegalArgumentException("not a typed durable payload"));
            CompiledDurablePayloadPlan plan = resolveStoredPayload(execution, slot, payload);
            return codec.decode(payload, plan);
        } catch (Exception error) {
            throw failure(execution, slot, "<resolved-from-release>", "decode", error);
        }
    }

    /** Decode-only compatibility path; legacy class hints cannot select a Java target. */
    public Object decodeLegacy(ExecutionRecord<?, ?> execution, Slot slot, String stored) {
        CompiledDurablePayloadPlan plan = resolve(execution, slot, execution.resultShape() == ExecutionResultShape.MATERIALIZED_MULTI && slot == Slot.RESULT);
        try {
            Object decoded = PipelineJson.mapper().readValue(stored, Object.class);
            Object body = decoded instanceof Map<?, ?> map && map.containsKey("_tpf_payload")
                ? map.get("_tpf_payload")
                : decoded;
            byte[] bytes = PipelineJson.mapper().writeValueAsBytes(body);
            return codec.decode(new TypedDurablePayload(plan.binding().canonicalTypeId(),
                plan.binding().typeExpressionFingerprint(), plan.binding().catalogFingerprint(),
                JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION, bytes), plan);
        } catch (Exception error) {
            throw failure(execution, slot, plan.binding().canonicalTypeId(), "legacy decode", error);
        }
    }

    private CompiledDurablePayloadPlan resolve(ExecutionRecord<?, ?> execution, Slot slot, boolean collection) {
        PipelineReleaseRecord release = pinnedRelease(execution);
        List<PipelineBundleStepDescriptor> steps = release.contract().steps();
        if (steps.isEmpty()) {
            throw new IllegalStateException("pinned release has no pipeline steps");
        }
        return resolve(release, execution, canonicalTypeId(steps, execution, slot), collection);
    }

    /**
     * Typed envelopes are self-identifying.  Cursor/slot semantics choose an expression only
     * when creating a value (or decoding legacy records); they must never reinterpret a typed
     * value after the execution has moved to a later step.
     */
    private CompiledDurablePayloadPlan resolveStoredPayload(
        ExecutionRecord<?, ?> execution,
        Slot slot,
        TypedDurablePayload payload) {
        PipelineReleaseRecord release = pinnedRelease(execution);
        StoredExpression stored = StoredExpression.parse(payload.canonicalTypeId());
        validatePermitted(release.contract().steps(), release.contract().canonicalTypes(), slot, stored.elementTypeId());
        return resolve(release, execution, stored.elementTypeId(), stored.collection());
    }

    private CompiledDurablePayloadPlan resolve(
        PipelineReleaseRecord release,
        ExecutionRecord<?, ?> execution,
        String requestedTypeId,
        boolean collection) {
        var resolvedDefinition = CanonicalPayloadBindingLookup.resolve(
            release.contract().canonicalTypes(), requestedTypeId);
        String canonicalTypeId = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
            .orElse(requestedTypeId);
        Map<String, Object> definition = resolvedDefinition
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::definition)
            .orElse(Map.of());
        String className;
        String definitionFingerprint;
        String catalog;
        if (resolvedDefinition.isPresent()) {
            className = required(definition, "runtimeClass", canonicalTypeId);
            definitionFingerprint = required(definition, "definitionFingerprint", canonicalTypeId);
            catalog = release.contract().canonicalCatalogFingerprint();
            if (catalog.isBlank()) {
                throw new IllegalStateException("pinned release canonical catalog fingerprint is unavailable");
            }
        } else if (release.contract().schemaVersion() == 1 && canonicalTypeId.contains(".")) {
            // Historical v1/v2 contracts stored fully-qualified Java identities rather than a
            // canonical catalog. The pinned release still chooses the target class.
            className = canonicalTypeId;
            definitionFingerprint = fingerprint(canonicalTypeId);
            catalog = release.contract().contractHash();
        } else {
            throw new IllegalStateException("pinned release has no canonical binding for " + canonicalTypeId);
        }
        String expression = collection ? fingerprint("List<" + definitionFingerprint + ">") : definitionFingerprint;
        DurablePayloadReleaseCoordinate coordinate = new DurablePayloadReleaseCoordinate(
            execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        var cached = plans.find(coordinate, expression);
        if (cached.isPresent()) {
            return cached.get();
        }
        Class<?> runtimeClass;
        try {
            runtimeClass = CanonicalPayloadRuntimeClassLoader.load(className, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException error) {
            throw new IllegalStateException("pinned release binding class is unavailable: " + className, error);
        }
        JavaType runtimeType = collection
            ? TypeFactory.defaultInstance().constructCollectionType(List.class, runtimeClass)
            : TypeFactory.defaultInstance().constructType(runtimeClass);
        String identity = collection ? "List<" + canonicalTypeId + ">" : canonicalTypeId;
        CanonicalPayloadBinding binding = new CanonicalPayloadBinding(identity, expression, catalog, collection ? List.class : runtimeClass, runtimeType);
        plans.activate(coordinate, Map.of(expression, binding));
        return plans.plan(coordinate, expression);
    }

    private PipelineReleaseRecord pinnedRelease(ExecutionRecord<?, ?> execution) {
        DurablePayloadReleaseCoordinate coordinate = new DurablePayloadReleaseCoordinate(
            execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        return releases.getOrLoad(new ReleaseCacheKey(execution.tenantId(), coordinate), ignored ->
            releaseRegistry.get(execution.tenantId(), execution.pipelineId(), execution.releaseVersion())
                .await().atMost(RELEASE_LOOKUP_TIMEOUT)
                .orElseThrow(() -> new IllegalStateException("pinned release is unavailable")));
    }

    private static void validatePermitted(
        List<PipelineBundleStepDescriptor> steps,
        Map<String, Map<String, Object>> definitions,
        Slot slot,
        String canonicalTypeId) {
        if (steps.isEmpty()) {
            throw new IllegalStateException("pinned release has no pipeline steps");
        }
        Set<String> permitted = switch (slot) {
            case INPUT, CONTINUATION_INPUT -> java.util.stream.Stream.concat(
                java.util.stream.Stream.of(steps.getFirst().inputTypeId()),
                steps.stream().map(PipelineBundleStepDescriptor::outputTypeId))
                .map(typeId -> canonicalIdentity(definitions, typeId))
                .collect(Collectors.toUnmodifiableSet());
            case RESULT -> steps.stream()
                .flatMap(step -> java.util.stream.Stream.of(step.inputTypeId(), step.outputTypeId()))
                .map(typeId -> canonicalIdentity(definitions, typeId))
                .collect(Collectors.toUnmodifiableSet());
        };
        if (!permitted.contains(canonicalTypeId)) {
            throw new IllegalStateException("Typed execution durable payload canonical type '" + canonicalTypeId
                + "' is not permitted for " + slot + "; permitted=" + permitted.stream().sorted().toList());
        }
    }

    private static String canonicalIdentity(Map<String, Map<String, Object>> definitions, String typeId) {
        return CanonicalPayloadBindingLookup.resolve(definitions, typeId)
            .map(CanonicalPayloadBindingLookup.ResolvedCanonicalDefinition::canonicalTypeId)
            .orElse(typeId);
    }

    private record StoredExpression(String elementTypeId, boolean collection) {
        private static StoredExpression parse(String canonicalTypeId) {
            if (canonicalTypeId.startsWith("List<") && canonicalTypeId.endsWith(">")) {
                String element = canonicalTypeId.substring("List<".length(), canonicalTypeId.length() - 1);
                if (element.isBlank()) {
                    throw new IllegalArgumentException("Typed durable payload list expression has no element type");
                }
                return new StoredExpression(element, true);
            }
            if (canonicalTypeId.isBlank()) {
                throw new IllegalArgumentException("Typed durable payload has no canonical type identity");
            }
            return new StoredExpression(canonicalTypeId, false);
        }
    }

    private record ReleaseCacheKey(String tenantId, DurablePayloadReleaseCoordinate coordinate) {
    }

    private static String canonicalTypeId(List<PipelineBundleStepDescriptor> steps, ExecutionRecord<?, ?> execution, Slot slot) {
        if (slot == Slot.INPUT) {
            // Execution input is immutable across retries, awaits, and later pipeline steps. Its
            // identity is the pipeline entry contract, never the currently resumed step.
            return steps.getFirst().inputTypeId();
        }
        if (slot == Slot.CONTINUATION_INPUT) {
            int current = execution.currentStepIndex();
            // A continuation is admitted to the step at the current cursor. In a branch
            // topology the preceding ordered step may belong to a different branch, so its
            // output is not the continuation contract. The receiving step's input is.
            return current >= 0 && current < steps.size()
                ? steps.get(current).inputTypeId()
                // Generated side-effect steps can extend the runtime cursor beyond the
                // semantic contract. At that boundary, itemized results re-enter through
                // the contract's terminal canonical output rather than the entry input.
                : steps.getLast().outputTypeId();
        }
        if (execution.resultShape() == ExecutionResultShape.MATERIALIZED_MULTI) {
            // Itemized child records persist the materialized output of their segment. Branch
            // ordering does not identify that result contract; the pinned pipeline contract's
            // terminal output does.
            return steps.getLast().outputTypeId();
        }
        int index = execution.currentStepIndex();
        return index >= 0 && index < steps.size()
            ? steps.get(index).inputTypeId()
            : steps.getLast().outputTypeId();
    }

    private static String required(Map<String, Object> definition, String field, String id) {
        Object value = definition.get(field);
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalStateException("pinned release canonical binding " + id + " does not declare " + field);
        }
        return string;
    }

    private static String fingerprint(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception error) {
            throw new IllegalStateException("Unable to fingerprint canonical type expression", error);
        }
    }

    private static IllegalStateException failure(ExecutionRecord<?, ?> execution, Slot slot, String canonicalTypeId, String action, Exception cause) {
        return new IllegalStateException("Execution durable payload " + action + " failed: executionId=" + execution.executionId()
            + ", release=" + execution.releaseVersion() + ", slot=" + slot + ", canonicalTypeId=" + canonicalTypeId, cause);
    }
}
