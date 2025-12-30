package org.pipelineframework.processor.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;

/**
 * Resolves REST binding information for a pipeline step.
 *
 * <p>Optional overrides are read from {@code application.properties} using:</p>
 * <ul>
 *   <li>{@code pipeline.rest.path.<ServiceName>}</li>
 *   <li>{@code pipeline.rest.path.<fully.qualified.ServiceClass>}</li>
 * </ul>
 */
public class RestBindingResolver {
    private static final String REST_PATH_PREFIX = "pipeline.rest.path.";
    private static final String APPLICATION_PROPERTIES_PATH = "src/main/resources/application.properties";

    /**
     * Resolves REST binding for a given PipelineStepModel using application.properties overrides.
     *
     * @param stepModel The semantic pipeline step definition
     * @param processingEnv The processing environment
     * @return A RestBinding with an optional path override
     */
    public RestBinding resolve(PipelineStepModel stepModel, ProcessingEnvironment processingEnv) {
        String override = null;
        try {
            Properties properties = loadApplicationProperties(processingEnv);
            if (!properties.isEmpty()) {
                override = resolveOverride(properties, stepModel);
            }
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                "Failed to read application.properties for REST path overrides: " + e.getMessage());
        }
        return new RestBinding(stepModel, override);
    }

    private String resolveOverride(Properties properties, PipelineStepModel stepModel) {
        String serviceNameKey = REST_PATH_PREFIX + stepModel.serviceName();
        if (properties.containsKey(serviceNameKey)) {
            return normalizeOverride(properties.getProperty(serviceNameKey));
        }

        String serviceClassKey = REST_PATH_PREFIX + stepModel.serviceClassName().canonicalName();
        if (properties.containsKey(serviceClassKey)) {
            return normalizeOverride(properties.getProperty(serviceClassKey));
        }

        return null;
    }

    private String normalizeOverride(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private Properties loadApplicationProperties(ProcessingEnvironment processingEnv) throws IOException {
        Properties properties = new Properties();
        for (Path baseDir : getBaseDirectories()) {
            Path propertiesPath = baseDir.resolve(APPLICATION_PROPERTIES_PATH);
            if (Files.exists(propertiesPath) && Files.isReadable(propertiesPath)) {
                try (InputStream input = Files.newInputStream(propertiesPath)) {
                    properties.load(input);
                    return properties;
                }
            }
        }

        try {
            var resource = processingEnv.getFiler()
                .getResource(javax.tools.StandardLocation.SOURCE_PATH, "", "application.properties");
            try (InputStream input = resource.openInputStream()) {
                properties.load(input);
            }
        } catch (Exception e) {
            // Ignore when the resource is not available.
        }

        return properties;
    }

    private Set<Path> getBaseDirectories() {
        Set<Path> baseDirs = new LinkedHashSet<>();
        String multiModuleDir = System.getProperty("maven.multiModuleProjectDirectory");
        if (multiModuleDir != null && !multiModuleDir.isBlank()) {
            baseDirs.add(Paths.get(multiModuleDir));
        }
        baseDirs.add(Paths.get(System.getProperty("user.dir", ".")));
        return baseDirs;
    }
}
