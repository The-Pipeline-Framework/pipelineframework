package org.pipelineframework.connector;

import java.util.List;
import java.util.Objects;

/**
 * Contents of {@code META-INF/pipeline/connector-providers.json}.
 */
public record ConnectorProviderManifest(int schemaVersion, List<ConnectorProviderArtifactDescriptor> providers) {
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public ConnectorProviderManifest {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                "unsupported connector provider manifest schema version " + schemaVersion + "; expected " + CURRENT_SCHEMA_VERSION);
        }
        providers = List.copyOf(Objects.requireNonNull(providers, "providers must not be null"));
        for (ConnectorProviderArtifactDescriptor provider : providers) {
            Objects.requireNonNull(provider, "provider artifact descriptor must not be null");
        }
    }
}
