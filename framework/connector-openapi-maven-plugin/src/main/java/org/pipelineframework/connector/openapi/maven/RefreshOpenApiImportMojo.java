package org.pipelineframework.connector.openapi.maven;

import java.io.IOException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.pipelineframework.connector.importer.ConnectorImportWriter;

/** Refreshes explicitly selected OpenAPI operations into committed Connector metadata. */
@Mojo(name = "refresh-import", requiresProject = true, threadSafe = false)
public final class RefreshOpenApiImportMojo extends AbstractOpenApiImportMojo {
    @Override
    public void execute() throws MojoExecutionException {
        try {
            var imported = new OpenApiImportEngine().importContract(load());
            ConnectorImportWriter.write(outputDirectory(), imported.provider(), imported.resources());
            getLog().info("Imported " + imported.provider().operations().size()
                + " OpenAPI operation(s) into release-pinned HTTP capabilities");
        } catch (IOException | RuntimeException failure) {
            throw new MojoExecutionException("Unable to refresh OpenAPI capability import", failure);
        }
    }
}
