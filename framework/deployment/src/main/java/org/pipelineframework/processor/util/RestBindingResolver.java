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

    /**
     * Creates a new RestBindingResolver.
     */
    public RestBindingResolver() {
    }
    private static final String REST_PATH_PREFIX = "pipeline.rest.path.";
    private static final String APPLICATION_PROPERTIES_PATH = "src/main/resources/application.properties";

    /**
     * Resolve the REST binding for a pipeline step, applying an override from application.properties when present.
     *
     * @param stepModel the pipeline step model whose REST binding should be resolved
     * @param processingEnv the annotation processing environment
     * @return a RestBinding whose path is overridden if an applicable property is defined, `null` override otherwise
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

    /**
     * Determine an override REST path for the given pipeline step by checking properties for
     * keys "pipeline.rest.path.{ServiceName}" then "pipeline.rest.path.{fully.qualified.ServiceClass}".
     *
     * @param properties properties to search for override entries
     * @param stepModel model describing the pipeline step whose service name and class are used as keys
     * @return the normalized override path if a matching property is present, otherwise null
     */
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

    /**
     * Normalises a potential override value by trimming surrounding whitespace.
     *
     * @param value the raw override value, may be null or contain surrounding whitespace
     * @return `null` if {@code value} is null or contains only whitespace, otherwise the trimmed string
     */
    private String normalizeOverride(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /**
     * Locate and load application.properties from candidate project base directories or the annotation-processor source resources.
     *
     * Attempts to read src/main/resources/application.properties from each directory returned by getBaseDirectories(); if not found there it tries to load application.properties via the processing environment's Filer. Returns the loaded Properties object or an empty Properties if no properties file is found.
     *
     * @param processingEnv the annotation-processing environment used to access resources via the Filer
     * @return the loaded Properties from application.properties, or an empty Properties if none was found
     * @throws IOException if an I/O error occurs while reading a properties file from the filesystem
     */
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

    /**
     * Produce an ordered set of candidate project base directories to search for resources.
     *
     * The set includes the `maven.multiModuleProjectDirectory` system property (if set and not blank)
     * followed by the current working directory (`user.dir`) or `"."` when `user.dir` is absent.
     *
     * @return an ordered Set of Paths representing candidate base directories; insertion order is preserved
     */
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
