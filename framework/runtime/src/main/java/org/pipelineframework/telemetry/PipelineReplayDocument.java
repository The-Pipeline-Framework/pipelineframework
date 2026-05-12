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
import java.util.List;

/**
 * Top-level replay export document.
 *
 * @param pipeline pipeline identifier
 * @param startedAt run start instant
 * @param durationMs run duration in milliseconds
 * @param status terminal run state
 * @param failureType terminal failure type when the run aborted
 * @param failureMessage terminal failure message when the run aborted
 * @param runParameters curated runtime configuration snapshot for the run
 * @param topology topology metadata used by the viewer
 * @param events replay events in sequence order
 */
public record PipelineReplayDocument(
    String pipeline,
    Instant startedAt,
    Long durationMs,
    String status,
    String failureType,
    String failureMessage,
    PipelineReplayRunParameters runParameters,
    PipelineReplayTopology topology,
    List<PipelineExecutionEvent> events
) {
    public PipelineReplayDocument {
        events = events == null ? List.of() : List.copyOf(events);
    }
}
