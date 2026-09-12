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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineTemplateUnionTest {

    @Test
    void rejectsNullName() {
        assertThrows(NullPointerException.class, () -> new PipelineTemplateUnion(null, Map.of()));
    }

    @Test
    void normalizesNullVariantsToEmptyMap() {
        assertEquals(Map.of(), new PipelineTemplateUnion("Outcome", null).variants());
    }

    @Test
    void defensivelyCopiesVariantsAndExposesAnUnmodifiableMap() {
        Map<String, PipelineTemplateUnionVariant> variants = new HashMap<>();
        PipelineTemplateUnionVariant captured = new PipelineTemplateUnionVariant("captured", "Captured", 1);
        variants.put("captured", captured);

        PipelineTemplateUnion union = new PipelineTemplateUnion("Outcome", variants);
        variants.put("rejected", new PipelineTemplateUnionVariant("rejected", "Rejected", 2));

        assertNotSame(variants, union.variants());
        assertEquals(Map.of("captured", captured), union.variants());
        assertThrows(UnsupportedOperationException.class, () -> union.variants().clear());
    }

    @Test
    void variantComponentsAndEqualityUseRecordValues() {
        PipelineTemplateUnionVariant first = new PipelineTemplateUnionVariant("captured", "Captured", 1);
        PipelineTemplateUnionVariant equal = new PipelineTemplateUnionVariant("captured", "Captured", 1);
        PipelineTemplateUnionVariant different = new PipelineTemplateUnionVariant("rejected", "Rejected", 2);

        assertEquals("captured", first.name());
        assertEquals("Captured", first.type());
        assertEquals(1, first.number());
        assertEquals(first, equal);
        assertEquals(first.hashCode(), equal.hashCode());
        assertNotEquals(first, different);
    }
}
