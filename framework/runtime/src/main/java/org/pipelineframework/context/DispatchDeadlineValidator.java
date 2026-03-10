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

package org.pipelineframework.context;

import java.util.Optional;

import io.grpc.Status;

/**
 * Enforces absolute dispatch deadlines at transport boundaries.
 */
public final class DispatchDeadlineValidator {
    private DispatchDeadlineValidator() {
    }

    /**
     * Rejects invocations whose absolute deadline has already elapsed.
     *
     * @param deadlineEpochMs absolute deadline in epoch milliseconds
     * @param boundary logical boundary name for diagnostics
     */
    public static void ensureNotExpired(Long deadlineEpochMs, String boundary) {
        if (deadlineEpochMs == null) {
            return;
        }
        long now = System.currentTimeMillis();
        if (deadlineEpochMs <= now) {
            throw Status.DEADLINE_EXCEEDED
                .withDescription(
                    "Dispatch deadline exceeded before invocation at " + boundary
                        + " (deadlineEpochMs=" + deadlineEpochMs
                        + ", nowEpochMs=" + now + ")")
                .asRuntimeException();
        }
    }

    /**
     * Rejects invocations whose absolute deadline has already elapsed.
     *
     * @param deadlineEpochMs absolute deadline in epoch milliseconds when present
     * @param boundary logical boundary name for diagnostics
     */
    public static void ensureNotExpired(Optional<Long> deadlineEpochMs, String boundary) {
        if (deadlineEpochMs == null || deadlineEpochMs.isEmpty()) {
            return;
        }
        ensureNotExpired(deadlineEpochMs.get(), boundary);
    }
}
