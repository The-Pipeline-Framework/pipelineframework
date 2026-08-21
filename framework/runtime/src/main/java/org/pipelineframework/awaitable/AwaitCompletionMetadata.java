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

package org.pipelineframework.awaitable;

import java.time.Instant;

/** Framework-authored facts about one admitted Await completion. */
public record AwaitCompletionMetadata(String interactionId, String actor, Instant completedAt) {
    public AwaitCompletionMetadata {
        if (interactionId == null || interactionId.isBlank()) {
            throw new IllegalArgumentException("interactionId must not be blank");
        }
        if (completedAt == null) {
            throw new IllegalArgumentException("completedAt must not be null");
        }
        actor = actor == null ? "" : actor.trim();
    }
}
