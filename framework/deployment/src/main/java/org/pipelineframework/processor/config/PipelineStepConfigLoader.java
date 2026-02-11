package org.pipelineframework.processor.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import org.pipelineframework.config.PlatformOverrideResolver;
import org.pipelineframework.config.TransportOverrideResolver;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads pipeline step configuration metadata from a YAML file.
 */
public class PipelineStepConfigLoader {
    private final Function<String, String> propertyLookup;
    private final Function<String, String> envLookup;
    private final Messager messager;

    /**
     * Construct a PipelineStepConfigLoader that uses system properties and environment variables.
     */
    public PipelineStepConfigLoader() {
        this(System::getProperty, System::getenv, null);
    }

    /**
     * Create a PipelineStepConfigLoader with injectable lookups for system properties and environment variables.
     *
     * @param propertyLookup function that accepts a property name and returns its value; if `null`, a lookup that always returns `null` is used
     * @param envLookup function that accepts an environment variable name and returns its value; if `null`, a lookup that always returns `null` is used
     */
    public PipelineStepConfigLoader(Function<String, String> propertyLookup, Function<String, String> envLookup) {
        this(propertyLookup, envLookup, null);
    }

    /**
     * Create a PipelineStepConfigLoader with injectable lookups and optional messager.
     *
     * @param propertyLookup function that accepts a property name and returns its value; if `null`, a lookup that always returns `null` is used
     * @param envLookup function that accepts an environment variable name and returns its value; if `null`, a lookup that always returns `null` is used
     * @param messager optional annotation processing messager for warnings
     */
    public PipelineStepConfigLoader(
            Function<String, String> propertyLookup,
            Function<String, String> envLookup,
            Messager messager) {
        this.propertyLookup = propertyLookup == null ? key -> null : propertyLookup;
        this.envLookup = envLookup == null ? key -> null : envLookup;
        this.messager = messager;
    }

    /**
     * Minimal step configuration extracted from the pipeline YAML.
     *
     * @param basePackage the configured base package
     * @param transport the configured transport name
     * @param platform the configured platform name
     * @param inputTypes the list of input type names declared in steps
     * @param outputTypes the list of output type names declared in steps
     */
    public record StepConfig(String basePackage, String transport, String platform, List<String> inputTypes, List<String> outputTypes) {
        /**
         * Backward-compatible constructor used by existing tests/callers.
         *
         * @param basePackage base package
         * @param transport transport
         * @param inputTypes input types
         * @param outputTypes output types
         */
        public StepConfig(String basePackage, String transport, List<String> inputTypes, List<String> outputTypes) {
            this(basePackage, transport, "COMPUTE", inputTypes, outputTypes);
        }
    }

    /**
     * Load pipeline step configuration from a YAML file.
     *
     * Reads the file at the given path, extracts the configured base package, resolves the transport
     * (applying known-name normalization and any external override), and collects declared step
     * input and output type names.
     *
     * @param configPath the path to the pipeline YAML configuration
     * @return a StepConfig containing the configured base package, the resolved transport name,
     *         the list of input type names, and the list of output type names
     * @throws IllegalStateException if the YAML file cannot be read
     */
    public StepConfig load(Path configPath) {
        Object root = loadYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            return new StepConfig("", "", List.of(), List.of());
        }

        String basePackage = getString(rootMap.get("basePackage"));
        String transport = getString(rootMap.get("transport"));
        String platform = getString(rootMap.get("platform"));
        String normalizedTransport = TransportOverrideResolver.normalizeKnownTransport(transport);
        if (normalizedTransport != null) {
            transport = normalizedTransport;
        } else if (transport != null && !transport.isBlank()) {
            warn("Unknown pipeline transport '" + transport + "' in step config; defaulting to GRPC.");
            transport = "GRPC";
        }
        String transportOverride = resolveTransportOverride();
        if (transportOverride != null && !transportOverride.isBlank()) {
            String normalizedOverride = TransportOverrideResolver.normalizeKnownTransport(transportOverride);
            if (normalizedOverride != null) {
                transport = normalizedOverride;
            } else {
                warn("Unknown pipeline.transport override '" + transportOverride
                    + "'; ignoring override and retaining existing value '" + transport + "'.");
            }
        }
        String normalizedPlatform = PlatformOverrideResolver.normalizeKnownPlatform(platform);
        if (normalizedPlatform != null) {
            platform = normalizedPlatform;
        } else {
            if (platform != null && !platform.isBlank()) {
                warn("Unknown pipeline platform '" + platform + "' in step config; defaulting to COMPUTE.");
            }
            platform = "COMPUTE";
        }
        String platformOverride = resolvePlatformOverride();
        if (platformOverride != null && !platformOverride.isBlank()) {
            String normalizedOverride = PlatformOverrideResolver.normalizeKnownPlatform(platformOverride);
            if (normalizedOverride != null) {
                platform = normalizedOverride;
            } else {
                warn("Unknown pipeline.platform override '" + platformOverride
                    + "'; ignoring override and retaining existing value '" + platform + "'.");
            }
        }
        Object stepsValue = rootMap.get("steps");
        if (!(stepsValue instanceof List<?> steps)) {
            return new StepConfig(basePackage, transport, platform, List.of(), List.of());
        }

        List<String> inputTypes = new ArrayList<>();
        List<String> outputTypes = new ArrayList<>();
        for (Object step : steps) {
            if (!(step instanceof Map<?, ?> stepMap)) {
                continue;
            }
            Object inputTypeName = stepMap.get("inputTypeName");
            if (inputTypeName != null) {
                inputTypes.add(String.valueOf(inputTypeName));
            }
            Object outputTypeName = stepMap.get("outputTypeName");
            if (outputTypeName != null) {
                outputTypes.add(String.valueOf(outputTypeName));
            }
        }

        return new StepConfig(basePackage, transport, platform, inputTypes, outputTypes);
    }

    private Object loadYaml(Path configPath) {
        Yaml yaml = new Yaml();
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config: " + configPath, e);
        }
    }

    /**
     * Convert a YAML value to its string representation.
     *
     * @param value the YAML value to convert; may be null
     * @return the string representation of {@code value}, or an empty string if {@code value} is null
     */
    private String getString(Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value);
    }

    /**
     * Resolve the transport override value from the configured property and environment lookups.
     *
     * @return the transport override string, or null/empty string if no override is configured
     */
    private String resolveTransportOverride() {
        return TransportOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }

    /**
     * Resolve platform override value from configured lookup functions.
     *
     * @return resolved platform override
     */
    private String resolvePlatformOverride() {
        return PlatformOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }

    private void warn(String message) {
        if (messager != null) {
            messager.printMessage(Diagnostic.Kind.WARNING, message);
        } else {
            System.err.println(Diagnostic.Kind.WARNING + ": " + message);
        }
    }
}
