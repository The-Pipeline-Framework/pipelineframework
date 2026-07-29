package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

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
    public enum Slot { INPUT, RESULT }

    @Inject PipelineReleaseRegistry releaseRegistry;
    @Inject JsonDurablePayloadCodec codec;

    private final DurablePayloadPlanRegistry plans = new DurablePayloadPlanRegistry();

    public String encode(ExecutionRecord<?, ?> execution, Slot slot, Object value) {
        CompiledDurablePayloadPlan plan = resolve(execution, slot, value instanceof List<?>);
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
            CompiledDurablePayloadPlan plan = resolve(execution, slot, payload.canonicalTypeId().startsWith("List<"));
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
        PipelineReleaseRecord release = releaseRegistry.get(execution.tenantId(), execution.pipelineId(), execution.releaseVersion())
            .await().indefinitely().orElseThrow(() -> new IllegalStateException("pinned release is unavailable"));
        List<PipelineBundleStepDescriptor> steps = release.contract().steps();
        if (steps.isEmpty()) {
            throw new IllegalStateException("pinned release has no pipeline steps");
        }
        String canonicalTypeId = canonicalTypeId(steps, execution, slot);
        Map<String, Object> definition = release.contract().canonicalTypes().get(canonicalTypeId);
        String className;
        String definitionFingerprint;
        String catalog;
        if (definition != null) {
            className = required(definition, "runtimeClass", canonicalTypeId);
            definitionFingerprint = required(definition, "definitionFingerprint", canonicalTypeId);
            catalog = release.contract().canonicalCatalogFingerprint();
            if (catalog.isBlank()) {
                throw new IllegalStateException("pinned release canonical catalog fingerprint is unavailable");
            }
        } else if (canonicalTypeId.contains(".")) {
            // Historical v1/v2 contracts stored fully-qualified Java identities rather than a
            // canonical catalog. The pinned release still chooses the target class.
            className = canonicalTypeId;
            definitionFingerprint = fingerprint(canonicalTypeId);
            catalog = release.contract().contractHash();
        } else {
            throw new IllegalStateException("pinned release has no canonical binding for " + canonicalTypeId);
        }
        Class<?> runtimeClass;
        try {
            runtimeClass = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException error) {
            throw new IllegalStateException("pinned release binding class is unavailable: " + className, error);
        }
        String expression = collection ? fingerprint("List<" + definitionFingerprint + ">") : definitionFingerprint;
        JavaType runtimeType = collection
            ? TypeFactory.defaultInstance().constructCollectionType(List.class, runtimeClass)
            : TypeFactory.defaultInstance().constructType(runtimeClass);
        String identity = collection ? "List<" + canonicalTypeId + ">" : canonicalTypeId;
        CanonicalPayloadBinding binding = new CanonicalPayloadBinding(identity, expression, catalog, collection ? List.class : runtimeClass, runtimeType);
        DurablePayloadReleaseCoordinate coordinate = new DurablePayloadReleaseCoordinate(
            execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        plans.activate(coordinate, Map.of(expression, binding));
        return plans.plan(coordinate, expression);
    }

    private static String canonicalTypeId(List<PipelineBundleStepDescriptor> steps, ExecutionRecord<?, ?> execution, Slot slot) {
        if (slot == Slot.INPUT) {
            int index = execution.currentStepIndex();
            if (index < 0 || index >= steps.size()) {
                throw new IllegalStateException("pinned release has no input contract at step index " + index);
            }
            return steps.get(index).inputTypeId();
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
