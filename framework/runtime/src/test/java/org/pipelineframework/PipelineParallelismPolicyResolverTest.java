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

import org.junit.jupiter.api.Test;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ParallelismHints;
import org.pipelineframework.parallelism.ThreadSafety;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineParallelismPolicyResolverTest {

    private final PipelineParallelismPolicyResolver resolver = new PipelineParallelismPolicyResolver();

    @Test
    void sequentialPolicyDisablesParallelization() {
        assertFalse(resolver.shouldParallelize(
            new RelaxedSafeHints(),
            ParallelismPolicy.SEQUENTIAL,
            PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_MANY));
    }

    @Test
    void parallelPolicyEnablesParallelization() {
        assertTrue(resolver.shouldParallelize(
            new RelaxedSafeHints(),
            ParallelismPolicy.PARALLEL,
            PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE));
    }

    @Test
    void autoPolicyHonorsHints() {
        assertTrue(resolver.shouldParallelize(
            new RelaxedSafeHints(),
            ParallelismPolicy.AUTO,
            PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE));
    }

    @Test
    void unsafeStepUnderNonSequentialPolicyThrows() {
        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> resolver.shouldParallelize(
                new UnsafeHints(),
                ParallelismPolicy.PARALLEL,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE));
        assertEquals(
            "Step " + UnsafeHints.class.getName() + " is not thread-safe; set pipeline.parallelism=SEQUENTIAL to proceed.",
            ex.getMessage());
    }

    @Test
    void strictRequiredOrderingUnderNonSequentialPolicyThrows() {
        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> resolver.shouldParallelize(
                new StrictRequiredHints(),
                ParallelismPolicy.PARALLEL,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE));
        assertEquals(
            "Step " + StrictRequiredHints.class.getName() + " requires strict ordering; set pipeline.parallelism=SEQUENTIAL to proceed.",
            ex.getMessage());
    }

    @Test
    void strictAdvisedOrderingFollowsCurrentBehavior() {
        assertFalse(resolver.shouldParallelize(
            new StrictAdvisedAnnotationStep(),
            ParallelismPolicy.AUTO,
            PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_MANY));
        assertTrue(resolver.shouldParallelize(
            new StrictAdvisedAnnotationStep(),
            ParallelismPolicy.PARALLEL,
            PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_MANY));
    }

    static class RelaxedSafeHints implements ParallelismHints {
        @Override
        public OrderingRequirement orderingRequirement() {
            return OrderingRequirement.RELAXED;
        }

        @Override
        public ThreadSafety threadSafety() {
            return ThreadSafety.SAFE;
        }
    }

    static class UnsafeHints extends RelaxedSafeHints {
        @Override
        public ThreadSafety threadSafety() {
            return ThreadSafety.UNSAFE;
        }
    }

    static class StrictRequiredHints extends RelaxedSafeHints {
        @Override
        public OrderingRequirement orderingRequirement() {
            return OrderingRequirement.STRICT_REQUIRED;
        }
    }

    @ParallelismHint(ordering = OrderingRequirement.STRICT_ADVISED, threadSafety = ThreadSafety.SAFE)
    static class StrictAdvisedAnnotationStep {
    }
}
