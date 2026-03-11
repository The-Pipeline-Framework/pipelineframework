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

package org.pipelineframework;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.pipeline.TestSteps;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineStepOrdererTest {

    private final PipelineStepOrderer orderer = new PipelineStepOrderer();

    @TempDir
    Path tempDir;

    @Test
    void preservesOriginalOrderWhenMetadataAbsentAndNotRequired() {
        List<Object> originalSteps = List.of(
            new TestSteps.TestStepOneToOneProcessed(),
            new TestSteps.TestStepOneToMany(),
            new TestSteps.TestStepManyToMany());

        List<Object> orderedSteps = withContextClassLoader(tempDir, () -> orderer.orderSteps(originalSteps));

        assertEquals(originalSteps, orderedSteps);
    }

    @Test
    void appliesConfiguredOrderCorrectly() {
        TestSteps.TestStepOneToOneProcessed stepB = new TestSteps.TestStepOneToOneProcessed();
        TestSteps.TestStepOneToMany stepA = new TestSteps.TestStepOneToMany();
        TestSteps.TestStepManyToMany stepC = new TestSteps.TestStepManyToMany();

        List<Object> ordered = orderer.applyConfiguredOrder(
            List.of(stepB, stepA, stepC),
            List.of(stepA.getClass().getName(), stepB.getClass().getName(), stepC.getClass().getName()));

        assertEquals(List.of(stepA, stepB, stepC), ordered);
    }

    @Test
    void preservesOriginalOrderWhenRuntimeListIncludesUnconfiguredSteps() {
        TestSteps.TestStepOneToOneProcessed stepB = new TestSteps.TestStepOneToOneProcessed();
        TestSteps.TestStepOneToMany stepA = new TestSteps.TestStepOneToMany();

        List<Object> ordered = orderer.applyConfiguredOrder(
            List.of(stepB, stepA),
            List.of(stepB.getClass().getName()));

        assertEquals(List.of(stepB, stepA), ordered);
    }

    @Test
    void appliesConfiguredOrderWithoutCollapsingDuplicateStepClasses() {
        TestSteps.TestStepOneToOneProcessed first = new TestSteps.TestStepOneToOneProcessed();
        TestSteps.TestStepOneToOneProcessed second = new TestSteps.TestStepOneToOneProcessed();

        List<Object> ordered = orderer.applyConfiguredOrder(
            List.of(first, second),
            List.of(first.getClass().getName(), second.getClass().getName()));

        assertEquals(List.of(first, second), ordered);
    }

    private static <T> T withContextClassLoader(Path root, java.util.concurrent.Callable<T> callable) {
        try (URLClassLoader classLoader = new URLClassLoader(
            new URL[] {root.toUri().toURL()},
            Thread.currentThread().getContextClassLoader())) {
            ClassLoader previous = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
            try {
                return callable.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
