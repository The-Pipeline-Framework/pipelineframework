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

package org.pipelineframework.config.template;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;

/**
 * Full pipeline template configuration loaded from the pipeline template YAML file.
 *
 * @param version template config schema version
 * @param appName application name
 * @param basePackage base Java package
 * @param transport global transport
 * @param platform runtime/deployment platform mode
 * @param messages top-level named messages
 * @param steps pipeline steps
 * @param aspects aspect configurations keyed by aspect name
 * @param input reliable pipeline input boundary
 * @param output reliable pipeline output boundary
 */
public record PipelineTemplateConfig(
    int version,
    String appName,
    String basePackage,
    String transport,
    PipelinePlatform platform,
    Map<String, PipelineTemplateMessage> messages,
    List<PipelineTemplateStep> steps,
    Map<String, PipelineTemplateAspect> aspects,
    PipelineInputBoundaryConfig input,
    PipelineOutputBoundaryConfig output
) {
    public PipelineTemplateConfig {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be > 0");
        }
        requireText(appName, "appName");
        requireText(basePackage, "basePackage");
        requireText(transport, "transport");
        if (platform == null) {
            throw new IllegalArgumentException("platform must not be null");
        }
        validateMap(messages, "messages");
        validateMap(aspects, "aspects");
        messages = messages == null ? Map.of() : Map.copyOf(messages);
        // Preserve null step placeholders so downstream phases/tests can explicitly skip them.
        steps = steps == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(steps));
        aspects = aspects == null ? Map.of() : Map.copyOf(aspects);
    }

    /**
     * Validates that the provided text is not null, empty, or only whitespace.
     *
     * @param value the string to validate
     * @param fieldName the name of the field used in the exception message
     * @throws IllegalArgumentException if {@code value} is null or blank
     */
    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    /**
     * Validates that the provided map contains no null keys or values.
     *
     * @param values the map to validate; may be {@code null}, in which case no validation is performed
     * @param fieldName the logical name of the field used in exception messages
     * @throws IllegalArgumentException if any key is {@code null} (message: "<fieldName> must not contain null keys")
     *                                  or any value is {@code null} (message: "<fieldName> must not contain null values")
     */
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
     * Constructs a PipelineTemplateConfig with backward-compatible defaults for newly added fields.
     *
     * Defaults applied: version = 1, platform = PipelinePlatform.COMPUTE, messages = empty map, no input/output boundaries.
     *
     * @param appName the application name
     * @param basePackage the base Java package
     * @param transport the global transport identifier
     * @param steps the pipeline steps (may contain null placeholders to preserve positions)
     * @param aspects aspect configurations keyed by aspect name
     */
    public PipelineTemplateConfig(
        String appName,
        String basePackage,
        String transport,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateAspect> aspects
    ) {
        this(1, appName, basePackage, transport, PipelinePlatform.COMPUTE, Map.of(), steps, aspects, null, null);
    }

    /**
     * Create a pipeline template configuration preset to version 1 with no messages and no input/output boundaries.
     *
     * @param appName     the application name for generated pipeline artifacts
     * @param basePackage the root Java package for generated code
     * @param transport   the transport identifier to use for the pipeline
     * @param platform    the target pipeline platform
     * @param steps       the ordered list of template steps; may contain null placeholders to indicate skipped positions
     * @param aspects     a map of aspect identifiers to their template definitions
     */
    public PipelineTemplateConfig(
        String appName,
        String basePackage,
        String transport,
        PipelinePlatform platform,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateAspect> aspects
    ) {
        this(1, appName, basePackage, transport, platform, Map.of(), steps, aspects, null, null);
    }
}
