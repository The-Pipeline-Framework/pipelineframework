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
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DispatchDeadlineValidatorTest {

    @Test
    void acceptsMissingDeadline() {
        assertDoesNotThrow(() -> DispatchDeadlineValidator.ensureNotExpired((Long) null, "unit"));
        assertDoesNotThrow(() -> DispatchDeadlineValidator.ensureNotExpired(Optional.empty(), "unit"));
    }

    @Test
    void acceptsFutureDeadline() {
        assertDoesNotThrow(() -> DispatchDeadlineValidator.ensureNotExpired(
            System.currentTimeMillis() + 60_000L,
            "unit"));
    }

    @Test
    void rejectsExpiredDeadline() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> DispatchDeadlineValidator.ensureNotExpired(System.currentTimeMillis() - 1_000L, "unit"));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ex.getStatus().getCode());
    }

    @Test
    void rejectsDeadlineAtExactCurrentTime() {
        long now = System.currentTimeMillis();
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> DispatchDeadlineValidator.ensureNotExpired(now, "unit"));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ex.getStatus().getCode());
    }
}
