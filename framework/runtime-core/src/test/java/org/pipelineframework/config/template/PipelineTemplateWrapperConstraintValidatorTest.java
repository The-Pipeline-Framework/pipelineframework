/*
 * Copyright (c) 2026 Mariano Barcia
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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class PipelineTemplateWrapperConstraintValidatorTest {
    @Test
    void rejectsAdjacentOverlappingQuantifiedAtomsBeforeMatchingAllowedValues() {
        assertUnsafe("[ab]*".repeat(80) + "z");
        assertUnsafe("\\d*\\d*z");
        assertUnsafe("\\x61*\\x61*z");
        assertEquals(Optional.empty(), PipelineTemplateWrapperConstraintValidator.findViolation(
            "string", constraints("\\d*[a-z]*z", "123abcz")));
    }

    private void assertUnsafe(String pattern) {
        assertEquals(PipelineTemplateWrapperConstraintValidator.Kind.UNSAFE_PATTERN,
            PipelineTemplateWrapperConstraintValidator.findViolation("string", constraints(
                pattern, "a".repeat(4_095) + "c")).orElseThrow().kind());
    }

    private PipelineTemplateWrapperConstraints constraints(String pattern, String allowedValue) {
        return new PipelineTemplateWrapperConstraints(
            Optional.empty(), Optional.of(4_096), Optional.of(pattern), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), List.of(allowedValue));
    }
}
