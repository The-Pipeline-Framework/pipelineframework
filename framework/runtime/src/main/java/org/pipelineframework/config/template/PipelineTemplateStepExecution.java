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

package org.pipelineframework.config.template;

/**
 * Remote/local execution metadata for a v2 template step.
 *
 * @param mode execution mode; defaults to {@code LOCAL} when absent
 * @param operatorId logical operator identifier used for remote routing and diagnostics
 * @param protocol remote execution protocol
 * @param timeoutMs optional per-step timeout cap in milliseconds
 * @param target remote target definition
 */
public record PipelineTemplateStepExecution(
    String mode,
    String operatorId,
    String protocol,
    Integer timeoutMs,
    PipelineTemplateRemoteTarget target
) {
    public PipelineTemplateStepExecution {
        mode = normalize(mode);
        operatorId = normalize(operatorId);
        protocol = normalize(protocol);
    }

    public boolean isRemote() {
        return "REMOTE".equalsIgnoreCase(mode);
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
