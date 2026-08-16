package org.pipelineframework.query;

import java.util.Objects;

import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;

/**
 * Shared operation-first selection model for a provider-backed Query step. Unlike deprecated
 * provider-first Command selection, operation/using Query selection always requires a binding.
 */
public record NativeQuerySelector(
    ConnectorBindingName binding,
    ConnectorOperationIdentity operationIdentity,
    int providerMajorVersion
) {
    public NativeQuerySelector {
        binding = Objects.requireNonNull(binding, "connector binding must not be null");
        operationIdentity = Objects.requireNonNull(operationIdentity, "query operation identity must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("query provider major version must be positive");
        }
    }
}
