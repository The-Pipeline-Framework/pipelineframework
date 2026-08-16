package org.pipelineframework.command;

import java.util.Optional;
import java.util.Objects;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;

/**
 * Runtime selection for one native command operation. The binding is present for operation/using
 * selections and intentionally absent for deprecated provider-first selection.
 */
public record NativeCommandSelector(
    Optional<ConnectorBindingName> binding,
    ConnectorOperationIdentity operationIdentity,
    int providerMajorVersion,
    CommandPolicy policy
) {
    public NativeCommandSelector {
        binding = Objects.requireNonNull(binding, "connector binding must not be null");
        operationIdentity = Objects.requireNonNull(operationIdentity, "command operation identity must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("command provider major version must be positive");
        }
        policy = Objects.requireNonNull(policy, "command policy must not be null");
    }

    public NativeCommandSelector(
        ConnectorOperationIdentity operationIdentity,
        int providerMajorVersion,
        CommandPolicy policy
    ) {
        this(Optional.empty(), operationIdentity, providerMajorVersion, policy);
    }

    public String commandName() {
        return binding.map(name -> "native-binding:" + name.value() + "/" + operationIdentity.operationId())
            .orElseGet(() -> "native:" + operationIdentity.providerId().value() + "/"
                + operationIdentity.operationId());
    }
}
