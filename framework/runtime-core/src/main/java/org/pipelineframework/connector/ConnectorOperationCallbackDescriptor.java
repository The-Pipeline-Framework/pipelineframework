package org.pipelineframework.connector;

import java.util.Objects;

/** A statically selected completion capability, with no endpoint or application authority. */
public record ConnectorOperationCallbackDescriptor(
    String id, ConnectorOperationTypeContract typeContract, boolean required
) {
    public ConnectorOperationCallbackDescriptor {
        id = ConnectorProviderId.require(id, "callback ID");
        Objects.requireNonNull(typeContract, "callback type contract must not be null");
        if (typeContract.outputType().isPresent()) {
            throw new IllegalArgumentException("callback type contract declares only its canonical input payload");
        }
    }
}
