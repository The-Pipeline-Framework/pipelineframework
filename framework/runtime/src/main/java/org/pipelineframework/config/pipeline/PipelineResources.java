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

/**
 * Shared utilities for pipeline resource loading operations.
 */
public final class PipelineResources {

    private PipelineResources() {
    }

    /**
     * Resolves the appropriate ClassLoader for resource loading.
     *
     * @return the resolved ClassLoader
     */
    public static ClassLoader resolveClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PipelineResources.class.getClassLoader();
        }
        return classLoader;
    }

    /**
     * Opens a resource using the provided ClassLoader with fallback mechanisms.
     *
     * @param classLoader the ClassLoader to use for resource loading
     * @param resource the resource path to open
     * @return the InputStream for the resource, or null if not found
     */
    public static InputStream openResource(ClassLoader classLoader, String resource) {
        InputStream stream = classLoader != null ? classLoader.getResourceAsStream(resource) : null;
        if (stream == null) {
            stream = PipelineResources.class.getResourceAsStream("/" + resource);
        }
        if (stream == null) {
            stream = ClassLoader.getSystemResourceAsStream(resource);
        }
        return stream;
    }
}