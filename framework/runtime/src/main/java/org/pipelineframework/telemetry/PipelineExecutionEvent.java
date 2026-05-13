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

import java.util.List;
import java.util.Map;

/**
 * Replay-friendly execution event.
 *
 * @param traceId distributed trace id
 * @param spanId step span id
 * @param parentSpanId parent span id
 * @param itemId logical item id
 * @param pipeline pipeline identifier
 * @param step logical step name
 * @param service service name
 * @param event semantic event type
 * @param startTime relative start time in seconds
 * @param endTime relative end time in seconds
 * @param durationMs duration in milliseconds
 * @param from source logical step name
 * @param to target logical step name
 * @param cardinality canonical cardinality
 * @param parentItemIds parent lineage ids for fan-in and fan-out
 * @param sequence monotonically increasing event sequence
 * @param attempt retry attempt number
 * @param errorType error type when present
 * @param errorMessage error message when present
 * @param attributes optional event attributes for viewer/runtime-specific semantics
 */
public record PipelineExecutionEvent(
    String traceId,
    String spanId,
    String parentSpanId,
    String itemId,
    String pipeline,
    String step,
    String service,
    String event,
    double startTime,
    double endTime,
    long durationMs,
    String from,
    String to,
    String cardinality,
    List<String> parentItemIds,
    Long sequence,
    Integer attempt,
    String errorType,
    String errorMessage,
    Map<String, String> attributes
) {
}
