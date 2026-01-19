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
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;

/**
 * Loads the pipeline execution order from the generated resource.
 */
public final class PipelineOrderResourceLoader {

    private static final String RESOURCE = "META-INF/pipeline/order.json";
    private static final String ROLES_RESOURCE = "META-INF/pipeline/roles.json";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = Logger.getLogger(PipelineOrderResourceLoader.class);

    private PipelineOrderResourceLoader() {
    }

    /**
     * Loads the pipeline execution order from the generated order resource.
     *
     * @return the ordered list of step class names, if available
     */
    public static Optional<List<String>> loadOrder() {
        ClassLoader classLoader = resolveClassLoader();
        InputStream stream = openResource(classLoader, RESOURCE);
        try (InputStream streamToRead = stream) {
            if (stream == null) {
                logMissingResource(classLoader);
                return Optional.empty();
            }
            Map<?, ?> data = MAPPER.readValue(streamToRead, Map.class);
            Object value = data.get("order");
            if (!(value instanceof List<?> list)) {
                throw new IllegalStateException("Pipeline order resource is missing an order array.");
            }
            List<String> order = list.stream()
                .filter(item -> item != null && !item.toString().isBlank())
                .map(Object::toString)
                .toList();
            return Optional.of(order);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read pipeline order resource.", e);
        }
    }

    /**
     * Returns whether an orchestrator is present and therefore order metadata is required.
     *
     * @return true if order metadata is required, false otherwise
     */
    public static boolean requiresOrder() {
        ClassLoader classLoader = resolveClassLoader();
        InputStream stream = openResource(classLoader, ROLES_RESOURCE);
        try (InputStream streamToRead = stream) {
            if (stream == null) {
                return false;
            }
            Map<?, ?> data = MAPPER.readValue(streamToRead, Map.class);
            Object roles = data.get("roles");
            if (!(roles instanceof Map<?, ?> rolesMap)) {
                return false;
            }
            Object orchestrator = rolesMap.get("ORCHESTRATOR_CLIENT");
            return orchestrator instanceof List<?> list && !list.isEmpty();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read pipeline roles resource.", e);
        }
    }

    private static ClassLoader resolveClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PipelineOrderResourceLoader.class.getClassLoader();
        }
        return classLoader;
    }

    private static InputStream openResource(ClassLoader classLoader, String resource) {
        InputStream stream = classLoader != null ? classLoader.getResourceAsStream(resource) : null;
        if (stream == null) {
            stream = PipelineOrderResourceLoader.class.getResourceAsStream("/" + resource);
        }
        if (stream == null) {
            stream = ClassLoader.getSystemResourceAsStream(resource);
        }
        return stream;
    }

    private static void logMissingResource(ClassLoader classLoader) {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        String loaderName = classLoader != null ? classLoader.getClass().getName() : "null";
        LOG.debugf("Pipeline order resource not found. classLoader=%s", loaderName);
        try {
            if (classLoader == null) {
                return;
            }
            Enumeration<java.net.URL> resources = classLoader.getResources(RESOURCE);
            if (!resources.hasMoreElements()) {
                LOG.debugf("No resources visible for %s", RESOURCE);
                return;
            }
            while (resources.hasMoreElements()) {
                LOG.debugf("Resource candidate: %s", resources.nextElement());
            }
        } catch (Exception e) {
            LOG.debug("Failed to enumerate pipeline order resources", e);
        }
    }
}
