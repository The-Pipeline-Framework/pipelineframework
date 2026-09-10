package org.pipelineframework.connector.openapi.maven;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.importer.ConnectorImportWriter;

/** Offline verification that committed import outputs match the vendored contract and selections. */
@Mojo(name = "verify-import", requiresProject = true, threadSafe = true)
public final class VerifyOpenApiImportMojo extends AbstractOpenApiImportMojo {
    @Override
    public void execute() throws MojoExecutionException {
        Path temporary = null;
        try {
            var imported = new OpenApiImportEngine().importContract(load());
            temporary = Files.createTempDirectory("tpf-openapi-verify-");
            Path currentManifest = outputDirectory().resolve(ConnectorProviderManifestLoader.RESOURCE_PATH);
            if (Files.isRegularFile(currentManifest)) {
                Path copied = temporary.resolve(ConnectorProviderManifestLoader.RESOURCE_PATH);
                Files.createDirectories(copied.getParent());
                Files.copy(currentManifest, copied);
            }
            ConnectorImportWriter.write(temporary, imported.provider(), imported.resources());
            for (var resource : imported.resources()) {
                equal(temporary.resolve(resource.path()), outputDirectory().resolve(resource.path()));
            }
            equal(temporary.resolve(ConnectorProviderManifestLoader.RESOURCE_PATH), currentManifest);
        } catch (IOException | RuntimeException failure) {
            throw new MojoExecutionException("Committed OpenAPI import is stale; run openapi:refresh-import", failure);
        } finally {
            if (temporary != null) {
                try (var paths = Files.walk(temporary)) {
                    paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            // Best-effort cleanup of an isolated verification directory.
                        }
                    });
                } catch (IOException ignored) {
                    // Best-effort cleanup of an isolated verification directory.
                }
            }
        }
    }

    private static void equal(Path expected, Path actual) throws IOException {
        if (!Files.isRegularFile(actual) || Files.mismatch(expected, actual) != -1) {
            throw new IllegalArgumentException("OpenAPI import output differs: " + actual);
        }
    }
}
