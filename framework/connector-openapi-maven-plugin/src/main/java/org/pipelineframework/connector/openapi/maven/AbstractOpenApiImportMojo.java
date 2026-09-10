package org.pipelineframework.connector.openapi.maven;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

abstract class AbstractOpenApiImportMojo extends AbstractMojo {
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    @Parameter(property = "openapi.importFile", defaultValue = "${project.basedir}/openapi-import.yaml", required = true)
    private File importFile;

    @Parameter(property = "openapi.outputDirectory", defaultValue = "${project.basedir}/src/main/resources", required = true)
    private File outputDirectory;

    final LoadedImport load() throws MojoExecutionException {
        try {
            Path configurationPath = importFile.toPath().toAbsolutePath().normalize();
            OpenApiImportConfiguration configuration = YAML.readValue(configurationPath.toFile(),
                OpenApiImportConfiguration.class);
            if (configuration.source == null || configuration.source.snapshot == null) {
                throw new IllegalArgumentException("OpenAPI import requires source.snapshot");
            }
            Path snapshot = configurationPath.getParent().resolve(configuration.source.snapshot).normalize();
            return new LoadedImport(configurationPath, configuration, OpenApiContractClosure.load(snapshot));
        } catch (IOException | RuntimeException failure) {
            throw new MojoExecutionException("Unable to load OpenAPI import configuration", failure);
        }
    }

    final Path outputDirectory() {
        return outputDirectory.toPath().toAbsolutePath().normalize();
    }

    record LoadedImport(
        Path configurationPath,
        OpenApiImportConfiguration configuration,
        OpenApiContractClosure.Resolved closure
    ) {
    }
}
