package org.pipelineframework.dispatch;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable exact capability catalogue for one generated dynamic operation adapter. */
public record OperationDispatchDescriptor(String stepId, Map<BoundOperationReference, DispatchCapability> capabilities) {
    public OperationDispatchDescriptor {
        stepId = Objects.requireNonNull(stepId, "dynamic operation step ID must not be null").trim();
        if (stepId.isEmpty()) {
            throw new IllegalArgumentException("dynamic operation step ID must not be blank");
        }
        capabilities = Map.copyOf(Objects.requireNonNull(capabilities, "dispatch capabilities must not be null"));
        if (capabilities.isEmpty()) {
            throw new IllegalArgumentException("dynamic operation step must expose at least one capability");
        }
        capabilities.forEach((reference, capability) -> {
            if (!reference.equals(capability.reference())) {
                throw new IllegalArgumentException("dispatch capability key does not match its reference");
            }
        });
    }

    public static OperationDispatchDescriptor of(String stepId, Collection<DispatchCapability> capabilities) {
        Map<BoundOperationReference, DispatchCapability> indexed = new LinkedHashMap<>();
        for (DispatchCapability capability : capabilities) {
            DispatchCapability duplicate = indexed.putIfAbsent(capability.reference(), capability);
            if (duplicate != null) {
                throw new IllegalArgumentException("duplicate bound dispatch capability: " + capability.reference());
            }
        }
        return new OperationDispatchDescriptor(stepId, indexed);
    }

    public DispatchCapability require(String binding, String operation) {
        BoundOperationReference reference = new BoundOperationReference(
            org.pipelineframework.connector.ConnectorBindingName.of(binding), operation);
        DispatchCapability capability = capabilities.get(reference);
        if (capability == null) {
            throw new IllegalArgumentException("operation is not exposed to dynamic operation step '" + stepId + "': "
                + reference.binding().value() + "/" + reference.operation());
        }
        return capability;
    }
}
