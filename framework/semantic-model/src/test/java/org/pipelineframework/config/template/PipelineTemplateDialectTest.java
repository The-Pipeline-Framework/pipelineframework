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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineTemplateDialectTest {

    @Test
    void exposesAllSupportedDialectsAndVersions() {
        assertEquals(3, PipelineTemplateDialect.values().length);
        assertEquals(1, PipelineTemplateDialect.V1.version());
        assertEquals(2, PipelineTemplateDialect.V2.version());
        assertEquals(3, PipelineTemplateDialect.V3.version());
    }

    @Test
    void resolvesEachSupportedVersion() {
        assertEquals(PipelineTemplateDialect.V1, PipelineTemplateDialect.fromVersion(1));
        assertEquals(PipelineTemplateDialect.V2, PipelineTemplateDialect.fromVersion(2));
        assertEquals(PipelineTemplateDialect.V3, PipelineTemplateDialect.fromVersion(3));
    }

    @Test
    void rejectsUnsupportedVersionsWithTheCurrentMessage() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> PipelineTemplateDialect.fromVersion(0)
        );

        assertEquals(
            "Unsupported pipeline template version: 0. Supported versions are 1, 2, and 3.",
            exception.getMessage()
        );
    }

    @Test
    void nullVersionFailsDuringPrimitiveUnboxing() {
        Integer version = null;

        assertThrows(NullPointerException.class, () -> PipelineTemplateDialect.fromVersion(version));
    }
}
