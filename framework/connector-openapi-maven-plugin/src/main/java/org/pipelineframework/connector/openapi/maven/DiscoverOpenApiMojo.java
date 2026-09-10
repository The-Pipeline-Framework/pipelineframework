package org.pipelineframework.connector.openapi.maven;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/** Emits a deterministic discovery report without importing or granting authority. */
@Mojo(name = "discover", requiresProject = true, threadSafe = true)
public final class DiscoverOpenApiMojo extends AbstractOpenApiImportMojo {
    @Parameter(property = "openapi.report", defaultValue = "${project.build.directory}/openapi-discovery.json", required = true)
    private File report;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            String discovery = new OpenApiImportEngine().discover(load().closure());
            var target = report.toPath().toAbsolutePath().normalize();
            Files.createDirectories(target.getParent());
            var temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
            try {
                Files.writeString(temporary, discovery, StandardCharsets.UTF_8);
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (IOException | RuntimeException failure) {
            throw new MojoExecutionException("Unable to discover OpenAPI operations", failure);
        }
    }
}
