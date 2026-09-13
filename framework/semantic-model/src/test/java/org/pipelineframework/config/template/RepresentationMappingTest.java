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

package org.pipelineframework.config.template;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RepresentationMappingTest {

    @Test
    void supportsBothConstructorsAndExposesNormalizedComponents() {
        RepresentationMapping mapping = new RepresentationMapping(
            " persistence ", " Invoice ", Optional.of(" com.example.Invoice "), Optional.of(" com.example.Mapper ")
        );
        RepresentationMapping withoutOptions = new RepresentationMapping(
            "query", "Invoice", Optional.empty(), Optional.empty(), null
        );

        assertEquals(" persistence ", mapping.key());
        assertEquals(" Invoice ", mapping.domainType());
        assertEquals(Optional.of("com.example.Invoice"), mapping.representationType());
        assertEquals(Optional.of("com.example.Mapper"), mapping.mapperType());
        assertEquals(Map.of(), mapping.options());
        assertEquals(Optional.empty(), withoutOptions.representationType());
        assertEquals(Optional.empty(), withoutOptions.mapperType());
        assertEquals(Map.of(), withoutOptions.options());
    }

    @Test
    void normalizesNullOptionalsAndCopiesOptionsIntoAnUnmodifiableMap() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("mode", "strict");

        RepresentationMapping mapping = new RepresentationMapping("query", "Invoice", null, null, options);
        options.put("mode", "changed");

        assertEquals(Optional.empty(), mapping.representationType());
        assertEquals(Optional.empty(), mapping.mapperType());
        assertNotSame(options, mapping.options());
        assertEquals(Map.of("mode", "strict"), mapping.options());
        assertThrows(UnsupportedOperationException.class, () -> mapping.options().clear());
        assertEquals(Map.of(), new RepresentationMapping("query", "Invoice", null, null, null).options());
    }

    @Test
    void reportsCurrentTextAndOptionalValidationErrors() {
        Map<String, Object> nullValueOptions = new LinkedHashMap<>();
        nullValueOptions.put("x", null);
        assertEquals("key must not be blank", assertThrows(
            IllegalArgumentException.class,
            () -> new RepresentationMapping(" ", "Invoice", Optional.empty(), Optional.empty())
        ).getMessage());
        assertEquals("domainType must not be blank", assertThrows(
            IllegalArgumentException.class,
            () -> new RepresentationMapping("query", null, Optional.empty(), Optional.empty())
        ).getMessage());
        assertEquals("representation mapping class name must not be blank", assertThrows(
            IllegalArgumentException.class,
            () -> new RepresentationMapping("query", "Invoice", Optional.of("  "), Optional.empty())
        ).getMessage());
        assertThrows(NullPointerException.class,
            () -> new RepresentationMapping("query", "Invoice", Optional.empty(), Optional.empty(), nullValueOptions));
    }
}
