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

package org.pipelineframework.processor.ir;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MapperFallbackMode enum.
 */
class MapperFallbackModeTest {

    @Test
    void enumHasExpectedValues() {
        MapperFallbackMode[] values = MapperFallbackMode.values();
        assertEquals(2, values.length);

        assertEquals(MapperFallbackMode.NONE, MapperFallbackMode.valueOf("NONE"));
        assertEquals(MapperFallbackMode.JACKSON, MapperFallbackMode.valueOf("JACKSON"));
    }

    @Test
    void enumValueOfIsCaseExact() {
        assertEquals(MapperFallbackMode.NONE, MapperFallbackMode.valueOf("NONE"));
        assertEquals(MapperFallbackMode.JACKSON, MapperFallbackMode.valueOf("JACKSON"));

        assertThrows(IllegalArgumentException.class, () -> MapperFallbackMode.valueOf("none"));
        assertThrows(IllegalArgumentException.class, () -> MapperFallbackMode.valueOf("jackson"));
        assertThrows(IllegalArgumentException.class, () -> MapperFallbackMode.valueOf("INVALID"));
    }

    @Test
    void enumHasCorrectOrdinals() {
        assertEquals(0, MapperFallbackMode.NONE.ordinal());
        assertEquals(1, MapperFallbackMode.JACKSON.ordinal());
    }

    @Test
    void enumToStringReturnsName() {
        assertEquals("NONE", MapperFallbackMode.NONE.toString());
        assertEquals("JACKSON", MapperFallbackMode.JACKSON.toString());
    }

    @Test
    void enumValuesAreStable() {
        // Verify that multiple calls to values() return the same enum constants
        MapperFallbackMode[] values1 = MapperFallbackMode.values();
        MapperFallbackMode[] values2 = MapperFallbackMode.values();

        assertSame(MapperFallbackMode.NONE, values1[0]);
        assertSame(MapperFallbackMode.JACKSON, values1[1]);
        assertSame(values1[0], values2[0]);
        assertSame(values1[1], values2[1]);
    }

    @Test
    void enumSupportsSwitch() {
        String result = switch (MapperFallbackMode.NONE) {
            case NONE -> "none";
            case JACKSON -> "jackson";
        };
        assertEquals("none", result);

        result = switch (MapperFallbackMode.JACKSON) {
            case NONE -> "none";
            case JACKSON -> "jackson";
        };
        assertEquals("jackson", result);
    }
}