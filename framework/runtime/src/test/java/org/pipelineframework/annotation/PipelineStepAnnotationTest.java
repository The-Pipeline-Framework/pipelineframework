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

package org.pipelineframework.annotation;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link PipelineStep} annotation contract.
 *
 * This class verifies structural properties of the annotation that change the codegen contract,
 * in particular changes introduced by this PR (removal of {@code runOnVirtualThreads}).
 */
class PipelineStepAnnotationTest {

    /**
     * Regression test: the {@code runOnVirtualThreads} element was removed from {@link PipelineStep}
     * in this PR. Virtual-thread offload is now configured in YAML, not on the annotation.
     * This test ensures the element stays absent so that generated code and consumers cannot
     * inadvertently re-introduce annotation-based virtual-thread selection.
     */
    @Test
    void pipelineStepAnnotationDoesNotHaveRunOnVirtualThreadsAttribute() {
        assertThrows(NoSuchMethodException.class,
            () -> PipelineStep.class.getDeclaredMethod("runOnVirtualThreads"),
            "runOnVirtualThreads must not exist on @PipelineStep; use YAML runOnVirtualThreads instead");
    }

    @Test
    void pipelineStepAnnotationRetainsOrderingAttribute() {
        assertDoesNotThrow(
            () -> PipelineStep.class.getDeclaredMethod("ordering"),
            "ordering must still be present on @PipelineStep");
    }

    @Test
    void pipelineStepAnnotationRetainsThreadSafetyAttribute() {
        assertDoesNotThrow(
            () -> PipelineStep.class.getDeclaredMethod("threadSafety"),
            "threadSafety must still be present on @PipelineStep");
    }

    @Test
    void pipelineStepAnnotationAttributeNamesDoNotIncludeRunOnVirtualThreads() {
        Set<String> attributeNames = Arrays.stream(PipelineStep.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        assertFalse(attributeNames.contains("runOnVirtualThreads"),
            "runOnVirtualThreads must not appear in @PipelineStep attribute names; found: " + attributeNames);
    }

    @Test
    void pipelineStepAnnotationHasExpectedCoreAttributes() {
        Set<String> attributeNames = Arrays.stream(PipelineStep.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        // Attributes that must remain for correct codegen and ordering/parallelism support
        assertTrue(attributeNames.contains("ordering"), "ordering must be present");
        assertTrue(attributeNames.contains("threadSafety"), "threadSafety must be present");
        assertTrue(attributeNames.contains("operator"), "operator must be present");
        assertTrue(attributeNames.contains("cacheKeyGenerator"), "cacheKeyGenerator must be present");
    }
}