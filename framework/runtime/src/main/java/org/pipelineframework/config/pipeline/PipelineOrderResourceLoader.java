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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jboss.logging.Logger;

/**
 * Loads the pipeline execution order from the generated resource.
 */
public final class PipelineOrderResourceLoader {

    private static final String RESOURCE = "META-INF/pipeline/order.json";
    private static final String ROLES_RESOURCE = "META-INF/pipeline/roles.json";
    private static final Logger LOG = Logger.getLogger(PipelineOrderResourceLoader.class);

    private PipelineOrderResourceLoader() {
    }

    /**
     * Loads the pipeline execution order from the generated order resource.
     *
     * @return the ordered list of step class names, if available
     */
    public static Optional<List<String>> loadOrder() {
        ClassLoader classLoader = PipelineResources.resolveClassLoader();
        try {
            if (classLoader != null) {
                Enumeration<java.net.URL> resources = classLoader.getResources(RESOURCE);
                List<List<String>> candidates = new ArrayList<>();
                while (resources.hasMoreElements()) {
                    java.net.URL url = resources.nextElement();
                    try (InputStream stream = url.openStream()) {
                        candidates.add(parseOrder(stream));
                    }
                }
                if (!candidates.isEmpty()) {
                    return Optional.of(selectBestOrderCandidate(candidates, classLoader));
                }
            }

            InputStream stream = PipelineResources.openResource(classLoader, RESOURCE);
            try (InputStream streamToRead = stream) {
                if (stream == null) {
                    logMissingResource(classLoader);
                    return Optional.empty();
                }
                return Optional.of(parseOrder(streamToRead));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read pipeline order resource.", e);
        }
    }

    private static List<String> parseOrder(InputStream stream) throws Exception {
        Map<?, ?> data = PipelineJson.mapper().readValue(stream, Map.class);
        Object value = data.get("order");
        if (!(value instanceof List<?> list)) {
            throw new IllegalStateException("Pipeline order resource is missing an order array.");
        }
        return list.stream()
            .filter(item -> item != null && !item.toString().isBlank())
            .map(Object::toString)
            .toList();
    }

    private static List<String> selectBestOrderCandidate(List<List<String>> candidates, ClassLoader classLoader) {
        List<String> best = candidates.getFirst();
        int bestLoadableCount = countLoadableSteps(best, classLoader);
        boolean bestAllLoadable = bestLoadableCount == best.size();

        for (int i = 1; i < candidates.size(); i++) {
            List<String> candidate = candidates.get(i);
            int loadableCount = countLoadableSteps(candidate, classLoader);
            boolean allLoadable = loadableCount == candidate.size();
            if (allLoadable && !bestAllLoadable) {
                best = candidate;
                bestLoadableCount = loadableCount;
                bestAllLoadable = true;
                continue;
            }
            if (allLoadable == bestAllLoadable && loadableCount > bestLoadableCount) {
                best = candidate;
                bestLoadableCount = loadableCount;
                bestAllLoadable = allLoadable;
            }
        }

        if (!bestAllLoadable) {
            LOG.warnf(
                "Selected pipeline order has unloadable steps (%d/%d loadable); verify duplicate order.json resources on classpath.",
                bestLoadableCount,
                best.size());
            return best.stream().filter(name -> isLoadable(name, classLoader)).toList();
        }
        return best;
    }

    private static int countLoadableSteps(List<String> order, ClassLoader classLoader) {
        int count = 0;
        for (String stepClass : order) {
            if (isLoadable(stepClass, classLoader)) {
                count++;
            }
        }
        return count;
    }

    private static boolean isLoadable(String stepClassName, ClassLoader classLoader) {
        if (stepClassName == null || stepClassName.isBlank()) {
            return false;
        }
        try {
            Class.forName(stepClassName, false, classLoader);
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

    /**
     * Returns whether an orchestrator is present and therefore order metadata is required.
     *
     * @return true if order metadata is required, false otherwise
     */
    public static boolean requiresOrder() {
        ClassLoader classLoader = PipelineResources.resolveClassLoader();
        InputStream stream = PipelineResources.openResource(classLoader, ROLES_RESOURCE);
        try (InputStream streamToRead = stream) {
            if (stream == null) {
                return false;
            }
            Map<?, ?> data = PipelineJson.mapper().readValue(streamToRead, Map.class);
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
