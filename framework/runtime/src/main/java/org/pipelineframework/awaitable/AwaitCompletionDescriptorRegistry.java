package org.pipelineframework.awaitable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;

/** Runtime registry of compiler-generated deferred-completion descriptors. */
@ApplicationScoped
public class AwaitCompletionDescriptorRegistry {
    private final Map<String, AwaitCompletionDescriptor> descriptors = new ConcurrentHashMap<>();

    public AwaitCompletionDescriptor register(AwaitCompletionDescriptor descriptor) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        AwaitCompletionDescriptor existing = descriptors.putIfAbsent(descriptor.stepId(), descriptor);
        if (existing == null) {
            return descriptor;
        }
        ensureCompatible(existing, descriptor);
        return existing;
    }

    public Uni<AwaitCompletionDescriptor> descriptorByStepId(String stepId) {
        return Uni.createFrom().item(() -> descriptorByStepIdNow(stepId));
    }

    public AwaitCompletionDescriptor descriptorByStepIdNow(String stepId) {
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        AwaitCompletionDescriptor descriptor = descriptors.get(stepId);
        if (descriptor == null) {
            throw new IllegalStateException(
                "No compiler-generated deferred-completion descriptor is registered for step '" + stepId + "'");
        }
        return descriptor;
    }

    private void ensureCompatible(
        AwaitCompletionDescriptor existing,
        AwaitCompletionDescriptor candidate
    ) {
        boolean compatible = existing.stepId().equals(candidate.stepId())
            && existing.inputType().equals(candidate.inputType())
            && existing.outputType().equals(candidate.outputType())
            && existing.cardinality().equals(candidate.cardinality())
            && existing.timeout().equals(candidate.timeout())
            && existing.correlationStrategy().equals(candidate.correlationStrategy())
            && existing.transportType().equals(candidate.transportType())
            && existing.transportConfig().equals(candidate.transportConfig())
            && existing.callback().equals(candidate.callback())
            && existing.idempotencyKeyFields().equals(candidate.idempotencyKeyFields())
            && existing.transportInputType().equals(candidate.transportInputType())
            && existing.transportOutputType().equals(candidate.transportOutputType())
            && java.util.Objects.equals(existing.completionProjectorId(), candidate.completionProjectorId())
            && existing.requestAwareCompletion() == candidate.requestAwareCompletion();
        if (!compatible) {
            throw new IllegalStateException(
                "Conflicting deferred-completion descriptors were registered for step '"
                    + candidate.stepId() + "'");
        }
    }
}
