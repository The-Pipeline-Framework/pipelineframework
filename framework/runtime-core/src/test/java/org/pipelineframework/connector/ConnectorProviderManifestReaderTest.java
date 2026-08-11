package org.pipelineframework.connector;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorProviderManifestReaderTest {

    @Test
    void readsStaticMetadataWithoutProviderConstruction() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":2},
            "configurationSchema":{"id":"metadata.provider.config","version":1},
            "operations":[{"id":"find","kind":"tpf:query","majorVersion":1}]}]}
            """));

        ConnectorProviderManifestCatalog catalog = new ConnectorProviderManifestCatalog(List.of(manifest));
        assertEquals("metadata.provider", catalog.providers().getFirst().provider().id().value());
        assertEquals(1, catalog.operations().size());
    }

    @Test
    void rejectsMalformedAndDuplicateStaticMetadataWithActionableDiagnostics() {
        IllegalArgumentException malformed = assertThrows(IllegalArgumentException.class, () -> ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[],"secret":"must-not-appear"}
            """)));
        assertTrue(malformed.getMessage().contains("unsupported field 'secret'"));

        ConnectorProviderManifest first = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"duplicate.metadata","version":{"major":1,"minor":0},"operations":[]}]}
            """));
        ConnectorProviderManifest second = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"duplicate.metadata","version":{"major":2,"minor":0},"operations":[]}]}
            """));
        IllegalArgumentException duplicate = assertThrows(
            IllegalArgumentException.class, () -> new ConnectorProviderManifestCatalog(List.of(first, second)));
        assertEquals("duplicate connector provider ID in static metadata: duplicate.metadata", duplicate.getMessage());
    }

    private static ByteArrayInputStream input(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }
}
