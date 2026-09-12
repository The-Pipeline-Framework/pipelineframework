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
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link PipelineStep} annotation contract.
 *
 * This class verifies the portable authored annotation contract.
 */
class PipelineStepAnnotationTest {

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
    void pipelineStepAnnotationHasRuntimeRetentionAndTypeTarget() {
        assertEquals(java.lang.annotation.RetentionPolicy.RUNTIME,
            PipelineStep.class.getAnnotation(java.lang.annotation.Retention.class).value());
        assertEquals(Set.of(java.lang.annotation.ElementType.TYPE),
            Set.of(PipelineStep.class.getAnnotation(java.lang.annotation.Target.class).value()));
    }

    @Test
    void pipelineStepDefaultsStayPortable() {
        PipelineStep annotation = MinimalStep.class.getAnnotation(PipelineStep.class);
        assertEquals(OrderingRequirement.RELAXED, annotation.ordering());
        assertEquals(ThreadSafety.SAFE, annotation.threadSafety());
        assertEquals(Void.class, annotation.cacheKeyGenerator());
    }

    @Test
    void pipelineStepAnnotationHasExpectedCoreAttributes() throws NoSuchMethodException {
        Set<String> attributeNames = Arrays.stream(PipelineStep.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        // Attributes that must remain for correct codegen and ordering/parallelism support
        assertTrue(attributeNames.contains("ordering"), "ordering must be present");
        assertTrue(attributeNames.contains("threadSafety"), "threadSafety must be present");
        assertTrue(attributeNames.contains("operator"), "operator must be present");
        assertTrue(attributeNames.contains("cacheKeyGenerator"), "cacheKeyGenerator must be present");
        assertEquals(Class.class, PipelineStep.class.getDeclaredMethod("cacheKeyGenerator").getReturnType());
    }

    @PipelineStep
    private static class MinimalStep {}
}
