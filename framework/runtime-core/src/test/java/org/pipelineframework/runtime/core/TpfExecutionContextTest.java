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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpfExecutionContextTest {

    @AfterEach
    void resetAdapters() {
        RuntimeAdapters.resetForTests();
    }

    @Test
    void storesNormalizedHeaderValues() {
        try (TpfExecutionContext.Scope ignored = TpfExecutionContext.withHeaders(
                " version-1 ", " replay ", " prefer-cache ")) {
            assertEquals("version-1", TpfExecutionContext.versionTag().orElseThrow());
            assertEquals("replay", TpfExecutionContext.replayMode().orElseThrow());
            assertEquals("prefer-cache", TpfExecutionContext.cachePolicy().orElseThrow());
        }
    }

    @Test
    void omitsBlankValues() {
        try (TpfExecutionContext.Scope ignored = TpfExecutionContext.withHeaders(" ", "", null)) {
            assertTrue(TpfExecutionContext.versionTag().isEmpty());
            assertTrue(TpfExecutionContext.replayMode().isEmpty());
            assertTrue(TpfExecutionContext.cachePolicy().isEmpty());
        }
    }

    @Test
    void restoresPreviousValuesWhenScopeCloses() {
        try (TpfExecutionContext.Scope outer = TpfExecutionContext.withHeaders("outer", null, "prefer-cache")) {
            try (TpfExecutionContext.Scope inner = TpfExecutionContext.withHeaders("inner", "replay", null)) {
                assertEquals("inner", TpfExecutionContext.versionTag().orElseThrow());
                assertEquals("replay", TpfExecutionContext.replayMode().orElseThrow());
                assertTrue(TpfExecutionContext.cachePolicy().isEmpty());
            }

            assertEquals("outer", TpfExecutionContext.versionTag().orElseThrow());
            assertTrue(TpfExecutionContext.replayMode().isEmpty());
            assertEquals("prefer-cache", TpfExecutionContext.cachePolicy().orElseThrow());
        }

        assertTrue(TpfExecutionContext.versionTag().isEmpty());
        assertTrue(TpfExecutionContext.replayMode().isEmpty());
        assertTrue(TpfExecutionContext.cachePolicy().isEmpty());
    }

    @Test
    void snapshotNormalizesValues() {
        TpfExecutionContext.Snapshot snapshot = new TpfExecutionContext.Snapshot(
            java.util.Optional.of(" version-1 "),
            java.util.Optional.of(" "),
            java.util.Optional.of(" prefer-cache "));

        assertEquals("version-1", snapshot.versionTag().orElseThrow());
        assertTrue(snapshot.replayMode().isEmpty());
        assertEquals("prefer-cache", snapshot.cachePolicy().orElseThrow());
    }
}
