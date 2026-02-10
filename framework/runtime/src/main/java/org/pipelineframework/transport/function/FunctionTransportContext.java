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

package org.pipelineframework.transport.function;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Context propagated at function transport boundaries.
 *
 * @param requestId external invocation/request identifier
 * @param functionName logical function name
 * @param stage logical stage (for example ingress, invoke-step, egress)
 * @param attributes free-form key/value attributes
 */
public record FunctionTransportContext(
    String requestId,
    String functionName,
    String stage,
    Map<String, String> attributes
) {
    /**
     * Creates a context with immutable attributes.
     */
    public FunctionTransportContext {
        Objects.requireNonNull(requestId, "requestId must not be null");
        functionName = functionName == null ? "" : functionName;
        stage = stage == null ? "" : stage;
        attributes = attributes == null
            ? Map.of()
            : Map.copyOf(new LinkedHashMap<>(attributes));
    }

    /**
     * Creates a basic context.
     *
     * @param requestId request id
     * @param functionName function name
     * @param stage stage name
     * @return context with no custom attributes
     */
    public static FunctionTransportContext of(String requestId, String functionName, String stage) {
        return new FunctionTransportContext(
            Objects.requireNonNull(requestId, "requestId must not be null"),
            Objects.requireNonNullElse(functionName, ""),
            Objects.requireNonNullElse(stage, ""),
            Map.of());
    }
}
