/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.telemetry;

import java.io.InputStream;
import java.util.Optional;

import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineResources;

/**
 * Loads generated replay topology metadata from {@code META-INF/pipeline/replay-topology.json}.
 */
public final class PipelineReplayTopologyLoader {

    private static final String RESOURCE = "META-INF/pipeline/replay-topology.json";

    private PipelineReplayTopologyLoader() {
    }

    /**
     * Loads replay topology metadata when present on the classpath.
     *
     * @return replay topology metadata
     */
    public static Optional<PipelineReplayTopology> load() {
        ClassLoader classLoader = PipelineResources.resolveClassLoader();
        InputStream stream = PipelineResources.openResource(classLoader, RESOURCE);
        try (InputStream streamToRead = stream) {
            if (streamToRead == null) {
                return Optional.empty();
            }
            return Optional.of(PipelineJson.mapper().readValue(streamToRead, PipelineReplayTopology.class));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read pipeline replay topology resource.", e);
        }
    }
}
