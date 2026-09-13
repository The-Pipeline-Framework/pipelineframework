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

package org.pipelineframework.processor.composition;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalPipelineInvocationClassNameTest {

    @Test
    void encodesTheCompleteCompiledLocationWithoutPunctuationCollisions() {
        String hyphenated = LocalPipelineInvocationClassName.simpleName(location("a-b"));
        String underscored = LocalPipelineInvocationClassName.simpleName(location("a_b"));

        assertNotEquals(hyphenated, underscored);
        assertTrue(hyphenated.startsWith("PipelineInvocation_"));
        assertTrue(underscored.startsWith("PipelineInvocation_"));
        assertTrue(hyphenated.chars().allMatch(character -> Character.isJavaIdentifierPart(character)));
        assertTrue(underscored.chars().allMatch(character -> Character.isJavaIdentifierPart(character)));
    }

    @Test
    void boundsLongUnicodeLocationNamesDeterministically() {
        CompiledPipelineLocation location = location("調査-🔁-".repeat(1_000));

        String first = LocalPipelineInvocationClassName.simpleName(location);
        String second = LocalPipelineInvocationClassName.simpleName(location);

        assertEquals(first, second);
        assertEquals("PipelineInvocation_".length() + 64, first.length());
        assertTrue(first.chars().allMatch(character -> Character.isJavaIdentifierPart(character)));
    }

    private static CompiledPipelineLocation location(String stepId) {
        return new CompiledPipelineLocation(
            List.of(),
            new DefinitionLocalLocation(new PipelineReference("root"), stepId));
    }
}
