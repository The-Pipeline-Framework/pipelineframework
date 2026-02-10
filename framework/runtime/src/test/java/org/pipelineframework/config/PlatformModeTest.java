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

package org.pipelineframework.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlatformModeTest {

    @Test
    void parsesCanonicalAndLegacyAliases() {
        assertEquals(PlatformMode.COMPUTE, PlatformMode.fromStringOptional("COMPUTE").orElseThrow());
        assertEquals(PlatformMode.COMPUTE, PlatformMode.fromStringOptional("STANDARD").orElseThrow());
        assertEquals(PlatformMode.COMPUTE, PlatformMode.fromStringOptional("compute").orElseThrow());
        assertEquals(PlatformMode.FUNCTION, PlatformMode.fromStringOptional("FUNCTION").orElseThrow());
        assertEquals(PlatformMode.FUNCTION, PlatformMode.fromStringOptional("LAMBDA").orElseThrow());
        assertEquals(PlatformMode.FUNCTION, PlatformMode.fromStringOptional("lambda").orElseThrow());
    }

    @Test
    void rejectsNullBlankAndUnknown() {
        assertTrue(PlatformMode.fromStringOptional(null).isEmpty());
        assertTrue(PlatformMode.fromStringOptional("").isEmpty());
        assertTrue(PlatformMode.fromStringOptional("UNKNOWN").isEmpty());
    }

    @Test
    void exposesExpectedPredicatesAndExternalNames() {
        assertTrue(PlatformMode.FUNCTION.isFunction());
        assertFalse(PlatformMode.FUNCTION.isCompute());
        assertTrue(PlatformMode.COMPUTE.isCompute());
        assertFalse(PlatformMode.COMPUTE.isFunction());
        assertEquals("FUNCTION", PlatformMode.FUNCTION.externalName());
        assertEquals("LAMBDA", PlatformMode.FUNCTION.legacyExternalName());
        assertEquals("COMPUTE", PlatformMode.COMPUTE.externalName());
        assertEquals("STANDARD", PlatformMode.COMPUTE.legacyExternalName());
    }

    @Test
    void legacyAliasHelpersRemainAvailable() {
        assertTrue(PlatformMode.FUNCTION.isLambda());
        assertTrue(PlatformMode.COMPUTE.isStandard());
    }
}
