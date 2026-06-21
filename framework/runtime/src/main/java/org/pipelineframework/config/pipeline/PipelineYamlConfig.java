/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.config.pipeline;

import java.util.List;
import java.util.Map;

import org.pipelineframework.config.PlatformOverrideResolver;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;

/**
 * Pipeline configuration parsed from pipeline.yaml.
 *
 * @param basePackage the base package for generated pipeline classes
 * @param transport the transport mode (GRPC, REST, or LOCAL)
 * @param platform the runtime/deployment platform mode (COMPUTE or FUNCTION)
 * @param steps the configured pipeline steps
 * @param sources named pipeline I/O sources
 * @param queries named captured query definitions
 * @param aspects the configured pipeline aspects
 * @param input the configured reliable pipeline input boundary
 * @param output the configured reliable pipeline output boundary
 */
public record PipelineYamlConfig(
    String basePackage,
    String transport,
    String platform,
    List<PipelineYamlStep> steps,
    Map<String, PipelineObjectSourceConfig> sources,
    Map<String, PipelineYamlQuery> queries,
    List<PipelineYamlAspect> aspects,
    PipelineInputBoundaryConfig input,
    PipelineOutputBoundaryConfig output
) {
    /**
     * Creates a validated pipeline configuration.
     *
     * @throws IllegalArgumentException when platform is not COMPUTE or FUNCTION
     */
    public PipelineYamlConfig {
        String normalizedPlatform = PlatformOverrideResolver.normalizeKnownPlatform(platform);
        if (normalizedPlatform == null) {
            if (platform == null || platform.isBlank()) {
                normalizedPlatform = "COMPUTE";
            } else {
                throw new IllegalArgumentException(
                    "Invalid platform '" + platform + "'. Allowed values: COMPUTE, FUNCTION "
                        + "(legacy aliases: STANDARD, LAMBDA).");
            }
        }
        platform = normalizedPlatform;
        validateMap(sources, "sources");
        validateMap(queries, "queries");
        sources = sources == null ? Map.of() : Map.copyOf(sources);
        queries = queries == null ? Map.of() : Map.copyOf(queries);
    }

    private static void validateMap(Map<?, ?> values, String fieldName) {
        if (values == null) {
            return;
        }
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException(fieldName + " must not contain null keys");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(fieldName + " must not contain null values");
            }
        }
    }

    /**
     * Backward-compatible constructor that defaults the platform to "COMPUTE".
     *
     * @param basePackage base package for generated pipeline classes
     * @param transport transport mode (e.g., "GRPC", "REST", or "LOCAL")
     * @param steps configured pipeline steps
     * @param aspects configured pipeline aspects
     */
    public PipelineYamlConfig(
        String basePackage,
        String transport,
        List<PipelineYamlStep> steps,
        List<PipelineYamlAspect> aspects
    ) {
        this(basePackage, transport, "COMPUTE", steps, Map.of(), Map.of(), aspects, null, null);
    }

    /**
     * Creates a PipelineYamlConfig with the given base package, transport, platform, steps, and aspects,
     * defaulting input/output boundaries to null for backward compatibility.
     *
     * @param basePackage base package
     * @param transport transport mode
     * @param platform platform mode
     * @param steps configured steps
     * @param aspects configured aspects
     */
    public PipelineYamlConfig(
        String basePackage,
        String transport,
        String platform,
        List<PipelineYamlStep> steps,
        List<PipelineYamlAspect> aspects
    ) {
        this(basePackage, transport, platform, steps, Map.of(), Map.of(), aspects, null, null);
    }

    public PipelineYamlConfig(
        String basePackage,
        String transport,
        String platform,
        List<PipelineYamlStep> steps,
        List<PipelineYamlAspect> aspects,
        PipelineInputBoundaryConfig input,
        PipelineOutputBoundaryConfig output
    ) {
        this(basePackage, transport, platform, steps, Map.of(), Map.of(), aspects, input, output);
    }

    public PipelineYamlConfig(
        String basePackage,
        String transport,
        String platform,
        List<PipelineYamlStep> steps,
        Map<String, PipelineObjectSourceConfig> sources,
        List<PipelineYamlAspect> aspects,
        PipelineInputBoundaryConfig input,
        PipelineOutputBoundaryConfig output
    ) {
        this(basePackage, transport, platform, steps, sources, Map.of(), aspects, input, output);
    }

    /**
     * Returns a copy of this config with the given transport while preserving existing boundaries.
     *
     * @param transport the transport to use in the returned config
     * @return a new PipelineYamlConfig with the updated transport
     */
    public PipelineYamlConfig withTransport(String transport) {
        return new PipelineYamlConfig(basePackage, transport, platform, steps, sources, queries, aspects, input, output);
    }

    /**
     * Returns a copy of this config with the given platform while preserving existing boundaries.
     *
     * @param platform the platform to use in the returned config
     * @return a new PipelineYamlConfig with the updated platform
     */
    public PipelineYamlConfig withPlatform(String platform) {
        return new PipelineYamlConfig(basePackage, transport, platform, steps, sources, queries, aspects, input, output);
    }
}
