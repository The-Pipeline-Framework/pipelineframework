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

package org.pipelineframework.context;

/**
 * Standard headers used for pipeline context propagation.
 */
public final class PipelineContextHeaders {

    /** Header name for pipeline version tags. */
    public static final String VERSION = "x-pipeline-version";
    /** Header name for pipeline replay mode. */
    public static final String REPLAY = "x-pipeline-replay";
    /** Header name for cache policy overrides. */
    public static final String CACHE_POLICY = "x-pipeline-cache-policy";
    /** Header name for cache status propagation. */
    public static final String CACHE_STATUS = "x-pipeline-cache-status";

    private PipelineContextHeaders() {
    }
}
