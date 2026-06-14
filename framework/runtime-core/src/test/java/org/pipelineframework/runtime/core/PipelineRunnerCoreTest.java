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

package org.pipelineframework.runtime.core;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineRunnerCoreTest {

    private final PipelineRunnerCore core = new PipelineRunnerCore();

    @Test
    void runSyncSequencesSelectedStepRange() {
        Object result = core.runSync(
            "a",
            List.of("b", "c", "d"),
            1,
            3,
            (step, current, index) -> current + ":" + index + ":" + step);

        assertEquals("a:1:c:2:d", result);
    }

    @Test
    void runSyncRejectsInvalidRange() {
        assertThrows(IllegalArgumentException.class,
            () -> core.runSync("a", List.of("b"), 1, 0, (step, current, index) -> current));
    }

    @Test
    void runSyncReportsSkippedNullSteps() {
        AtomicInteger skippedIndex = new AtomicInteger(-1);

        Object result = core.runSync(
            "a",
            Arrays.asList("b", null, "d"),
            0,
            3,
            (step, current, index) -> current + ":" + step,
            skippedIndex::set);

        assertEquals("a:b:d", result);
        assertEquals(1, skippedIndex.get());
    }

    @Test
    void runAsyncSequencesCompletionStageSteps() throws Exception {
        Object result = core.runAsync(
                "pay",
                List.of("validate", "approve"),
                (step, current, index) -> CompletableFuture.completedFuture(current + ":" + step))
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

        assertEquals("pay:validate:approve", result);
    }

    @Test
    void runAsyncSkipsNullSteps() throws Exception {
        Object result = core.runAsync(
                "start",
                Arrays.asList("step1", null, "step2"),
                (step, current, index) -> CompletableFuture.completedFuture(current + ":" + step))
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

        assertEquals("start:step1:step2", result);
    }
}
