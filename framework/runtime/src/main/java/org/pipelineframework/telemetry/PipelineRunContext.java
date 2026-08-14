/*
 * Copyright (c) 2026 Mariano Barcia
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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Runtime context for one pipeline execution; lifecycle state is owned by {@link PipelineRunLifecycle}. */
public record PipelineRunContext(
    String runId,
    Context context,
    Span span,
    long startNanos,
    Instant startedAt,
    Attributes attributes,
    boolean enabled,
    AtomicLong inflightCurrent,
    AtomicLong inflightMax,
    LongAdder inflightSamples,
    LongAdder inflightSum,
    LongAdder itemsConsumed,
    LongAdder itemsProduced,
    PipelineReplayRunParameters runParameters,
    ExecutionReplayTracker.RunReplayState replayState,
    AtomicBoolean endSignalled
) {
    static PipelineRunContext disabled() {
        return new PipelineRunContext(
            "disabled",
            Context.current(),
            null,
            0L,
            Instant.EPOCH,
            Attributes.empty(),
            false,
            new AtomicLong(),
            new AtomicLong(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            null,
            null,
            new AtomicBoolean(true));
    }
}
