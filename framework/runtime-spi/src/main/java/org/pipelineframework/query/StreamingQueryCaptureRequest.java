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

package org.pipelineframework.query;

import java.util.Objects;

/** Immutable identity and canonical input for one streaming Query capture. */
public record StreamingQueryCaptureRequest(
    String tenantId,
    String executionId,
    int stepIndex,
    String queryId,
    String queryVersion,
    String captureKey,
    String inputJson,
    String outputType
) {
    public StreamingQueryCaptureRequest {
        tenantId = requireText(tenantId, "tenantId");
        executionId = requireText(executionId, "executionId");
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be non-negative");
        }
        queryId = requireText(queryId, "queryId");
        queryVersion = requireText(queryVersion, "queryVersion");
        captureKey = requireText(captureKey, "captureKey");
        inputJson = Objects.requireNonNull(inputJson, "inputJson must not be null");
        outputType = requireText(outputType, "outputType");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
