package org.pipelineframework.connector;

import java.util.Objects;

/** Durable callback identity supplied to application-owned endpoint and authentication policies. */
public record ProviderCallbackRequest(String tenantId, String interactionId,
    ConnectorOperationIdentity operation, String callbackId) {
    public ProviderCallbackRequest {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(interactionId, "interactionId");
        Objects.requireNonNull(operation, "operation");
        callbackId = ConnectorProviderId.require(callbackId, "callbackId");
        if (tenantId.isBlank() || interactionId.isBlank()
            || tenantId.length() > 1024 || interactionId.length() > 1024) {
            throw new IllegalArgumentException("invalid callback identity");
        }
    }

    @Override
    public String toString() {
        return "ProviderCallbackRequest[operation=" + operation + ", callbackId=" + callbackId + "]";
    }
}
