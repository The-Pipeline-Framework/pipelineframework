package org.pipelineframework.awaitable;

import java.util.Map;
import java.util.Objects;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationCallbackDescriptor;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;

/** Release-owned callback selection. Runtime addresses and credentials are deliberately absent. */
public record ConnectorCallbackSelection(
    ConnectorBindingName binding,
    ConnectorOperationIdentity operation,
    int providerMajorVersion,
    ConnectorOperationCallbackDescriptor callback,
    String endpointResolverClass,
    String authenticatorClass
) {
    public ConnectorCallbackSelection {
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(callback, "callback");
        if (!operation.kind().equals(ConnectorOperationKind.COMMAND) || providerMajorVersion < 1) {
            throw new IllegalArgumentException("callback selection requires a versioned native Command");
        }
        endpointResolverClass = requireClass(endpointResolverClass);
        authenticatorClass = requireClass(authenticatorClass);
    }

    private static String requireClass(String name) {
        Objects.requireNonNull(name, "callback bean class");
        if (name.isBlank()) {
            throw new IllegalArgumentException("callback bean class must not be blank");
        }
        return name;
    }

    public Map<String, Object> contractMetadata() {
        return Map.of("binding", binding.value(), "provider", operation.providerId().value(),
            "providerVersion", providerMajorVersion, "operation", operation.operationId(),
            "operationVersion", operation.majorVersion(), "callback", callback.id(),
            "payloadType", callback.typeContract().inputType(), "endpointResolver", endpointResolverClass,
            "authenticator", authenticatorClass, "required", callback.required());
    }
}
