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

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared ObjectMapper provider for pipeline JSON operations.
 */
public final class PipelineJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PipelineJson() {
    }

    /**
     * Returns the shared ObjectMapper instance.
     *
     * @return the shared ObjectMapper instance, configured for pipeline JSON operations
     */
    public static ObjectMapper mapper() {
        return MAPPER;
    }
}