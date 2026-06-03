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

import java.time.Instant;

/**
 * Pluggable exporter for runtime execution replay events.
 */
public interface PipelineReplayExporter {

    default void runStarted(
        String runId,
        String pipeline,
        Instant startedAt,
        PipelineReplayRunParameters runParameters,
        PipelineReplayTopology topology
    ) {
    }

    default void emit(String runId, PipelineExecutionEvent event) {
    }

    default void emitControlEvent(
        String pipeline,
        Instant occurredAt,
        PipelineReplayTopology topology,
        PipelineExecutionEvent event) {
    }

    default void runCompleted(
        String runId,
        String pipeline,
        Instant startedAt,
        long durationMs,
        PipelineReplayTopology topology) {
    }

    default void runFailed(
        String runId,
        String pipeline,
        Instant startedAt,
        long durationMs,
        PipelineReplayTopology topology,
        Throwable failure) {
    }
}
