package org.pipelineframework.connector.maven;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectorProviderArtifacts;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;

class GenerateConnectorProviderArtifactsMojoTest {
    @TempDir
    Path classes;

    @Test
    void removesArtifactsLeftByAProviderThatNoLongerExists() throws Exception {
        Path service = classes.resolve(ConnectorProviderArtifacts.SERVICE_PATH);
        Path manifest = classes.resolve(ConnectorProviderManifestLoader.RESOURCE_PATH);
        Files.createDirectories(service.getParent());
        Files.createDirectories(manifest.getParent());
        Files.writeString(service, "removed.Provider\n");
        Files.writeString(manifest, "{\"schemaVersion\":1,\"providers\":[]}");

        GenerateConnectorProviderArtifactsMojo.deleteGeneratedArtifacts(classes);

        assertFalse(Files.exists(service));
        assertFalse(Files.exists(manifest));
    }
}
