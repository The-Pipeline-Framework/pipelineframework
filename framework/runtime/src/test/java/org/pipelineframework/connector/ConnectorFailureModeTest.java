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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;

class ConnectorFailureModeTest {

    @Test
    void enumContainsPropagateMode() {
        ConnectorFailureMode mode = ConnectorFailureMode.PROPAGATE;
        assertEquals("PROPAGATE", mode.name());
    }

    @Test
    void enumContainsLogAndContinueMode() {
        ConnectorFailureMode mode = ConnectorFailureMode.LOG_AND_CONTINUE;
        assertEquals("LOG_AND_CONTINUE", mode.name());
    }

    @Test
    void valueOfReturnsCorrectMode() {
        assertEquals(ConnectorFailureMode.PROPAGATE,
            ConnectorFailureMode.valueOf("PROPAGATE"));
        assertEquals(ConnectorFailureMode.LOG_AND_CONTINUE,
            ConnectorFailureMode.valueOf("LOG_AND_CONTINUE"));
    }

    @Test
    void valuesReturnsAllModes() {
        EnumSet<ConnectorFailureMode> modes = EnumSet.allOf(ConnectorFailureMode.class);
        assertTrue(modes.contains(ConnectorFailureMode.PROPAGATE));
        assertTrue(modes.contains(ConnectorFailureMode.LOG_AND_CONTINUE));
    }
}
