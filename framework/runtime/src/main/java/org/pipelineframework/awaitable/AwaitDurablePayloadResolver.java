package org.pipelineframework.awaitable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.orchestrator.CanonicalPayloadBinding;
import org.pipelineframework.orchestrator.CompiledDurablePayloadPlan;
import org.pipelineframework.orchestrator.DurablePayloadPlanRegistry;
import org.pipelineframework.orchestrator.DurablePayloadReleaseCoordinate;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Resolves pinned-release plans for the canonical request/response fields of await interactions. */
@ApplicationScoped
public class AwaitDurablePayloadResolver {
    public enum Slot { REQUEST, RESPONSE }

    @Inject ExecutionStateStore executionStateStore;
    @Inject PipelineReleaseRegistry releaseRegistry;
    @Inject AwaitStepDescriptorFactory descriptors;
    @Inject JsonDurablePayloadCodec codec;

    private final DurablePayloadPlanRegistry plans = new DurablePayloadPlanRegistry();

    public String encode(AwaitInteractionRecord interaction, Slot slot, Object value) {
        CompiledDurablePayloadPlan plan = resolve(interaction, slot);
        try {
            return PipelineJson.mapper().writeValueAsString(codec.encode(value, plan));
        } catch (Exception e) {
            throw failure(interaction, slot, plan.binding().canonicalTypeId(), "encode", e);
        }
    }

    public Object decode(AwaitInteractionRecord interaction, Slot slot, String stored) {
        try {
            TypedDurablePayload payload = TypedDurablePayload.fromSerializedBytes(stored.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(() -> new IllegalArgumentException("not a typed durable payload"));
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
        ExecutionRecord<Object, Object> execution = executionStateStore.getExecution(interaction.tenantId(), interaction.executionId())
            .await().indefinitely().orElseThrow(() -> new IllegalStateException("owning execution is unavailable"));
        PipelineReleaseRecord release = releaseRegistry.get(interaction.tenantId(), execution.pipelineId(), execution.releaseVersion())
            .await().indefinitely().orElseThrow(() -> new IllegalStateException("pinned release is unavailable"));
        var step = release.contract().steps().stream().filter(candidate -> candidate.index() == interaction.stepIndex()).findFirst()
            .orElseThrow(() -> new IllegalStateException("pinned release does not contain await step index " + interaction.stepIndex()));
        String canonicalTypeId = slot == Slot.REQUEST ? step.inputTypeId() : step.outputTypeId();
        Map<String, Object> canonicalDefinition = release.contract().canonicalTypes().get(canonicalTypeId);
        String runtimeType;
        String expression;
        String catalog;
        if (canonicalDefinition != null) {
            runtimeType = requiredString(canonicalDefinition, "runtimeClass", canonicalTypeId);
            expression = requiredString(canonicalDefinition, "definitionFingerprint", canonicalTypeId);
            catalog = release.contract().canonicalCatalogFingerprint();
            if (catalog.isBlank()) {
                throw new IllegalStateException("pinned release canonical catalog fingerprint is unavailable");
            }
        } else if (release.contract().schemaVersion() == 1) {
            AwaitStepDescriptor descriptor = descriptors.descriptorByStepIdNow(interaction.stepId());
            runtimeType = slot == Slot.REQUEST ? descriptor.inputType() : interaction.outputType();
            expression = fingerprint(canonicalTypeId);
            catalog = release.contract().contractHash();
        } else {
            throw new IllegalStateException("pinned release has no canonical binding for " + canonicalTypeId);
        }
        Class<?> runtimeClass;
        try {
            runtimeClass = Class.forName(runtimeType, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("pinned release binding class is unavailable: " + runtimeType, e);
        }
        DurablePayloadReleaseCoordinate coordinate = new DurablePayloadReleaseCoordinate(
            execution.pipelineId(), execution.contractVersion(), execution.releaseVersion());
        CanonicalPayloadBinding binding = new CanonicalPayloadBinding(canonicalTypeId, expression,
            catalog, runtimeClass);
        plans.activate(coordinate, Map.of(expression, binding));
        return plans.plan(coordinate, expression);
    }

    private static IllegalStateException failure(AwaitInteractionRecord interaction, Slot slot, String type, String action, Exception cause) {
        return new IllegalStateException("Await durable payload " + action + " failed: interactionId="
            + interaction.interactionId() + ", executionId=" + interaction.executionId() + ", slot=" + slot
            + ", canonicalTypeId=" + type, cause);
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
