package org.pipelineframework.connector.importer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestReader;
import org.pipelineframework.connector.ConnectorProviderVersion;

class ConnectorImportWriterTest {
    @TempDir
    Path temporary;

    @Test
    void replacesOnlyTheOwningProviderAndPublishesTheManifestLast() throws Exception {
        ConnectorImportWriter.write(temporary, provider("mcp.client", "read.old"), List.of(
            new ConnectorImportResource("META-INF/pipeline/mcp-tools.json", "old")));

        ConnectorImportWriter.write(temporary, provider("http.client", "evidence.lookup"), List.of(
            new ConnectorImportResource("META-INF/pipeline/http-operations.json", "pins"),
            new ConnectorImportResource("META-INF/pipeline/connector-operation-provenance.json", "provenance")));

        var manifest = ConnectorProviderManifestReader.read(Files.newInputStream(
            temporary.resolve("META-INF/pipeline/connector-providers.json")));
        assertEquals(List.of("http.client", "mcp.client"), manifest.providers().stream()
            .map(value -> value.provider().id().value()).toList());
        assertEquals("old", Files.readString(temporary.resolve("META-INF/pipeline/mcp-tools.json")));
        assertEquals("pins", Files.readString(temporary.resolve("META-INF/pipeline/http-operations.json")));

        ConnectorImportWriter.write(temporary, provider("mcp.client", "read.new"), List.of(
            new ConnectorImportResource("META-INF/pipeline/mcp-tools.json", "new")));
        var replaced = ConnectorProviderManifestReader.read(Files.newInputStream(
            temporary.resolve("META-INF/pipeline/connector-providers.json")));
        assertEquals(List.of("read.new"), replaced.providers().stream()
            .filter(value -> value.provider().id().value().equals("mcp.client"))
            .flatMap(value -> value.operations().stream()).map(ConnectorOperationDescriptor::id).toList());
    }

    @Test
    void rejectsDuplicateOrEscapingPrivateResources() {
        var resource = new ConnectorImportResource("META-INF/pipeline/pins.json", "one");
        assertThrows(IllegalArgumentException.class, () -> ConnectorImportWriter.write(
            temporary, provider("http.client", "read"), List.of(resource, resource)));
        assertThrows(IllegalArgumentException.class,
            () -> new ConnectorImportResource("META-INF/pipeline/../credentials", "secret"));
    }

    private static ConnectorProviderArtifactDescriptor provider(String id, String operation) {
        return new ConnectorProviderArtifactDescriptor(new ConnectorProviderDescriptor(
            ConnectorProviderId.of(id), new ConnectorProviderVersion(1, 0), Optional.empty()),
            List.of(new ConnectorOperationDescriptor(operation, ConnectorOperationKind.QUERY, 1)));
    }
}
