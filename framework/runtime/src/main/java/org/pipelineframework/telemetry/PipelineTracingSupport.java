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

package org.pipelineframework.telemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import java.util.LinkedHashMap;
import java.util.Map;

/** Shared tracing infrastructure: context capture and durable parent/link restoration only. */
public final class PipelineTracingSupport {
    private PipelineTracingSupport() { }

    public static Map<String, Object> captureCurrentContext() {
        return capture(Span.current().getSpanContext());
    }

    public static Map<String, Object> capture(SpanContext context) {
        if (context == null || !context.isValid()) return Map.of();
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("tpf.trace.id", context.getTraceId());
        metadata.put("tpf.trace.span_id", context.getSpanId());
        metadata.put("tpf.trace.flags", context.getTraceFlags().asHex());
        return Map.copyOf(metadata);
    }

    public static SpanContext durableOrigin(Map<String, Object> metadata) {
        if (metadata == null) return SpanContext.getInvalid();
        Object traceId = metadata.get("tpf.trace.id");
        Object spanId = metadata.get("tpf.trace.span_id");
        Object flags = metadata.get("tpf.trace.flags");
        if (!(traceId instanceof String trace) || !(spanId instanceof String span)) return SpanContext.getInvalid();
        return SpanContext.createFromRemoteParent(trace, span,
            TraceFlags.fromHex(flags instanceof String value ? value : "01", 0), TraceState.getDefault());
    }

    public static boolean same(SpanContext first, SpanContext second) {
        return first != null && second != null && first.isValid() && second.isValid()
            && first.getTraceId().equals(second.getTraceId()) && first.getSpanId().equals(second.getSpanId());
    }
}
