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
import org.jboss.logging.Logger;

/**
 * Loads the pipeline platform metadata from the generated resource.
 */
public final class PipelinePlatformResourceLoader {

    private static final String RESOURCE = "META-INF/pipeline/platform.json";
    private static final Logger LOG = Logger.getLogger(PipelinePlatformResourceLoader.class);

    private PipelinePlatformResourceLoader() {
    }

    /**
     * Loads the pipeline platform metadata from the generated platform resource.
     *
     * @return the platform metadata, if available
     */
    public static Optional<PlatformMetadata> loadPlatform() {
        ClassLoader classLoader = resolveClassLoader();
        InputStream stream = openResource(classLoader, RESOURCE);
        try (InputStream streamToRead = stream) {
            if (stream == null) {
                LOG.debugf("Pipeline platform resource not found: %s", RESOURCE);
                return Optional.empty();
            }
            Map<?, ?> data = PipelineJson.mapper().readValue(streamToRead, Map.class);
            String platform = readString(data, "platform");
            String transport = readString(data, "transport");
            String module = readString(data, "module");
            boolean pluginHost = readBoolean(data, "pluginHost");
            if (platform == null || platform.isBlank() || transport == null || transport.isBlank()) {
                LOG.warnf("Pipeline platform resource missing required fields (platform=%s, transport=%s)", platform, transport);
                return Optional.empty();
            }
            return Optional.of(new PlatformMetadata(platform, transport, module, pluginHost));
        } catch (Exception e) {
            LOG.warn("Failed to read pipeline platform resource.", e);
            return Optional.empty();
        }
    }

    /**
     * Platform metadata record containing build-time pipeline configuration.
     *
     * NOTE: This record is duplicated from the private static class in
     * org.pipelineframework.processor.util.PipelinePlatformMetadataGenerator.
     * Keep both definitions in sync when making changes.
     *
     * @param platform the target deployment platform (e.g. COMPUTE, FUNCTION)
     * @param transport the transport mode (e.g. GRPC, REST, LOCAL)
     * @param module the logical module name
     * @param pluginHost whether this module is a plugin host
     */
    public record PlatformMetadata(String platform, String transport, String module, boolean pluginHost) {
    }

    private static String readString(Map<?, ?> data, String key) {
        Object value = data.get(key);
        return value == null ? null : value.toString();
    }

    private static boolean readBoolean(Map<?, ?> data, String key) {
        Object value = data.get(key);
        if (value instanceof Boolean flag) {
            return flag;
        }
        return value != null && Boolean.parseBoolean(value.toString());
    }

    private static ClassLoader resolveClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PipelinePlatformResourceLoader.class.getClassLoader();
        }
        return classLoader;
    }

    private static InputStream openResource(ClassLoader classLoader, String resource) {
        InputStream stream = classLoader != null ? classLoader.getResourceAsStream(resource) : null;
        if (stream == null) {
            stream = PipelinePlatformResourceLoader.class.getResourceAsStream("/" + resource);
        }
        if (stream == null) {
            stream = ClassLoader.getSystemResourceAsStream(resource);
        }
        return stream;
    }
}
