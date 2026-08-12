package org.pipelineframework.connector;

import java.util.Objects;
import java.util.List;

/**
 * Identifies provider-defined, generated or derived configuration schema metadata.
 */
public record ConnectorConfigSchemaDescriptor(String id, int version, List<ConnectorConfigFieldDescriptor> fields) {
    public ConnectorConfigSchemaDescriptor {
        id = ConnectorProviderId.require(id, "configuration schema ID");
        if (version < 1) {
            throw new IllegalArgumentException("configuration schema version must be positive");
        }
        fields = List.copyOf(Objects.requireNonNull(fields, "configuration schema fields must not be null"));
    }

    public ConnectorConfigSchemaDescriptor(String id, int version) {
        this(id, version, List.of());
    }
}
