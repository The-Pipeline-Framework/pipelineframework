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

package org.pipelineframework.connector;

/**
 * Determines when connector dedupe state becomes authoritative.
 */
public enum ConnectorIdempotencyPolicy {
    /**
     * Performs no connector-side dedupe.
     * Downstream delivery remains at-least-once and duplicates may be forwarded.
     */
    DISABLED,

    /**
     * Marks a handoff key as accepted before forwarding downstream.
     * This suppresses duplicates aggressively but can drop work if delivery fails after reservation.
     */
    PRE_FORWARD,

    /**
     * Reserves the handoff key until the target accepts the record, then commits it as accepted.
     * This reduces lost-work risk but may retry and re-forward duplicates after delivery failures.
     */
    ON_ACCEPT
}
