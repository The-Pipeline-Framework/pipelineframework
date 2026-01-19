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
 * Request-scoped context propagated across pipeline calls.
 *
 * @param versionTag version identifier used for replay or cache versioning
 * @param replayMode replay mode indicator
 * @param cachePolicy cache policy override
 */
public record PipelineContext(String versionTag, String replayMode, String cachePolicy) {

    /**
     * Builds a PipelineContext from header values, trimming blanks to null.
     *
     * @param versionTag version identifier used for replay or cache versioning
     * @param replayMode replay mode indicator
     * @param cachePolicy cache policy override
     * @return a normalized PipelineContext instance
     */
    public static PipelineContext fromHeaders(String versionTag, String replayMode, String cachePolicy) {
        return new PipelineContext(normalize(versionTag), normalize(replayMode), normalize(cachePolicy));
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
