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

import org.pipelineframework.awaitable.AwaitCompletionMetrics;

/**
 * Replay-friendly control-plane event for durable await unit lifecycle changes.
 */
public record AwaitReplayLifecycleEvent(
    String eventName,
    String executionId,
    String unitId,
    String stepId,
    Integer stepIndex,
    String status,
    String interactionId,
    String correlationId,
    String transport,
    Integer itemIndex,
    Integer expectedItemCount,
    Integer completedItemCount,
    Boolean dispatchComplete
) implements AwaitCompletionMetrics.AwaitReplayView {
    public static final String INTERACTION_DISPATCHED = "await_interaction_dispatched";
    public static final String ADMISSION_ACQUIRED = "await_admission_acquired";
    public static final String ADMISSION_REUSED = "await_admission_reused";
    public static final String ADMISSION_RELEASED = "await_admission_released";
    public static final String ADMISSION_RECONCILED = "await_admission_reconciled";
    public static final String UNIT_DISPATCH_COMPLETE = "await_unit_dispatch_complete";
    public static final String EXECUTION_WAITING = "await_execution_waiting";
    public static final String UNIT_ITEM_COMPLETED = "await_unit_item_completed";
    public static final String UNIT_COMPLETED = "await_unit_completed";
    public static final String RESUME_RELEASED = "await_resume_released";
    public static final String UNIT_TERMINAL = "await_unit_terminal";

    public AwaitReplayLifecycleEvent {
        if (eventName == null || eventName.isBlank()) {
            throw new IllegalArgumentException("eventName must not be blank");
        }
        if (unitId == null || unitId.isBlank()) {
            throw new IllegalArgumentException("unitId must not be blank");
        }
    }
}
