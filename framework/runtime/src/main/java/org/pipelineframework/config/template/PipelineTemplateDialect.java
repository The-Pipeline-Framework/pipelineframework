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

/** The explicitly supported pipeline-template dialects. */
public enum PipelineTemplateDialect {
    V1(1),
    V2(2),
    V3(3);

    private final int version;

    PipelineTemplateDialect(int version) {
        this.version = version;
    }

    public int version() {
        return version;
    }

    public static PipelineTemplateDialect fromVersion(int version) {
        for (PipelineTemplateDialect dialect : values()) {
            if (dialect.version == version) {
                return dialect;
            }
        }
        throw new IllegalStateException("Unsupported pipeline template version: " + version + ". Supported versions are 1, 2, and 3.");
    }
}
