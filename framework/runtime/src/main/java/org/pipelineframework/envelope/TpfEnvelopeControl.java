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

package org.pipelineframework.envelope;

import java.util.Map;

/**
 * Strict framework-owned control metadata for an envelope boundary.
 *
 * @param pipeline pipeline/release/context label known at dispatch time
 * @param step generated step name
 * @param operatorId logical remote operator id
 * @param context optional execution/correlation metadata, omitted from the map when absent
 * @param metadata additional strict control metadata
 */
public record TpfEnvelopeControl(
    String pipeline,
    String step,
    String operatorId,
    Map<String, String> context,
    Map<String, String> metadata
) {
    public TpfEnvelopeControl {
        pipeline = requireText(pipeline, "pipeline");
        step = requireText(step, "step");
        operatorId = requireText(operatorId, "operatorId");
        context = context == null ? Map.of() : Map.copyOf(context);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
