package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Identifies provider-defined, generated or derived configuration schema metadata.
 */
public record ConnectorConfigSchemaDescriptor(String id, int version) {
    public ConnectorConfigSchemaDescriptor {
        id = ConnectorProviderId.require(id, "configuration schema ID");
        if (version < 1) {
            throw new IllegalArgumentException("configuration schema version must be positive");
        }
    }
}
