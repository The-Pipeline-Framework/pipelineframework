package org.pipelineframework.command;

import java.util.Objects;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorOperationIdentity;

/**
 * Runtime selection for one native command operation.
 */
public record NativeCommandSelector(
    ConnectorOperationIdentity operationIdentity,
    int providerMajorVersion,
    CommandPolicy policy
) {
    public NativeCommandSelector {
        operationIdentity = Objects.requireNonNull(operationIdentity, "command operation identity must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("command provider major version must be positive");
        }
        policy = Objects.requireNonNull(policy, "command policy must not be null");
    }

    public String commandName() {
        return operationIdentity.providerId().value() + "/" + operationIdentity.operationId();
    }
}
