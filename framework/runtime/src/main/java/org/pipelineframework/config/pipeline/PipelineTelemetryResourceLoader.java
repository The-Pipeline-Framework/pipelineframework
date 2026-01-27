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

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Loads item-boundary telemetry metadata from the generated resource.
 */
public final class PipelineTelemetryResourceLoader {

    private static final String RESOURCE = "META-INF/pipeline/telemetry.json";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PipelineTelemetryResourceLoader() {
    }

    /**
     * Loads the item-boundary metadata if available.
     *
     * @return telemetry metadata, if present
     */
    public static Optional<ItemBoundary> loadItemBoundary() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PipelineTelemetryResourceLoader.class.getClassLoader();
        }
        InputStream stream = classLoader != null ? classLoader.getResourceAsStream(RESOURCE) : null;
        if (stream == null) {
            stream = PipelineTelemetryResourceLoader.class.getResourceAsStream("/" + RESOURCE);
        }
        if (stream == null) {
            stream = ClassLoader.getSystemResourceAsStream(RESOURCE);
        }
        try (InputStream streamToRead = stream) {
            if (streamToRead == null) {
                return Optional.empty();
            }
            Map<?, ?> data = MAPPER.readValue(streamToRead, Map.class);
            String itemInputType = valueAsString(data.get("itemInputType"));
            String itemOutputType = valueAsString(data.get("itemOutputType"));
            String producerStep = valueAsString(data.get("producerStep"));
            String consumerStep = valueAsString(data.get("consumerStep"));
            Map<String, String> stepParents = mapOf(data.get("stepParents"));
            if (itemInputType == null || itemInputType.isBlank()
                || itemOutputType == null || itemOutputType.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(new ItemBoundary(itemInputType, itemOutputType, producerStep, consumerStep, stepParents));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read pipeline telemetry resource.", e);
        }
    }

    private static String valueAsString(Object value) {
        return value == null ? null : value.toString();
    }

    private static Map<String, String> mapOf(Object value) {
        if (!(value instanceof Map<?, ?> raw)) {
            return Map.of();
        }
        return raw.entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(java.util.stream.Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue().toString()));
    }

    /**
     * Item-boundary metadata resolved at build time.
     *
     * @param itemInputType configured input item type
     * @param itemOutputType configured output item type
     * @param producerStep step class that produces the output item type
     * @param consumerStep step class that consumes the input item type
     * @param stepParents mapping of plugin steps to their parent step classes
     */
    public record ItemBoundary(
        String itemInputType,
        String itemOutputType,
        String producerStep,
        String consumerStep,
        Map<String, String> stepParents) {
    }
}
