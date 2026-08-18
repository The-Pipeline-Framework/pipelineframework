package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ConnectorProviderManifestLoaderTest {

    @Test
    void rejectsBootstrapLoadedMetadataAnchor() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestLoader.metadataClassLoader(Object.class));

        assertEquals("metadata class loader anchor must not be bootstrap-loaded", failure.getMessage());
    }
}
