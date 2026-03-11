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

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineStepExecutorTest {

    @Test
    void oneToOneOnUniExecutesAndPropagatesContext() {
        PipelineContext context = new PipelineContext("v1", "tenant", "prefer-cache");

        String value = PipelineStepExecutor.withPipelineContext(
            context,
            () -> PipelineContextHolder.get().replayMode() + ":payload");

        assertEquals("tenant:payload", value);
    }

    @Test
    void oneToOneOnMultiSequentialProducesAllItems() {
        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            new SuffixOneToOneStep("-done"),
            Multi.createFrom().items("a", "b"),
            false,
            16,
            null,
            null,
            null,
            null);

        assertEquals(List.of("a-done", "b-done"), ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void oneToOneFutureOnMultiParallelProducesAllItems() {
        Object result = PipelineStepExecutor.applyOneToOneFutureUnchecked(
            new FutureSuffixStep("-future"),
            Multi.createFrom().items("a", "b"),
            true,
            16,
            null,
            null,
            null);

        List<String> values = ((Multi<String>) result).collect().asList().await().atMost(Duration.ofSeconds(5));
        assertTrue(values.containsAll(Set.of("a-future", "b-future")));
    }

    @Test
    void oneToManyMergeProducesExpandedItems() {
        Object result = PipelineStepExecutor.applyOneToManyUnchecked(
            new ExpandingOneToManyStep(),
            Uni.createFrom().item("x"),
            true,
            16,
            null,
            null,
            null);

        assertEquals(List.of("x-1", "x-2"), ((Multi<String>) result).collect().asList().await().indefinitely());
    }

    @Test
    void manyToOneFromMultiReducesItems() {
        Object result = PipelineStepExecutor.applyManyToOneUnchecked(
            (ManyToOne<String, String>) input -> input.collect().asList().map(items -> String.join(",", items)),
            Multi.createFrom().items("a", "b"),
            null,
            null,
            null);

        assertEquals("a,b", ((Uni<String>) result).await().indefinitely());
    }

    @Test
    void manyToManyFromUniProducesStream() {
        Object result = PipelineStepExecutor.applyManyToManyUnchecked(
            new MappingManyToManyStep(),
            Uni.createFrom().item("x"),
            null,
            null,
            null);

        assertEquals(List.of("x-mapped"), ((Multi<String>) result).collect().asList().await().indefinitely());
    }

    static final class SuffixOneToOneStep extends ConfigurableStep implements StepOneToOne<String, String> {
        private final String suffix;

        SuffixOneToOneStep(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom().item(input + suffix);
        }
    }

    static final class FutureSuffixStep extends ConfigurableStep implements StepOneToOneCompletableFuture<String, String> {
        private final String suffix;

        FutureSuffixStep(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public CompletableFuture<String> applyAsync(String in) {
            return CompletableFuture.completedFuture(in + suffix);
        }
    }

    static final class ExpandingOneToManyStep extends ConfigurableStep implements StepOneToMany<String, String> {
        @Override
        public Multi<String> applyOneToMany(String in) {
            return Multi.createFrom().items(in + "-1", in + "-2");
        }
    }

    static final class MappingManyToManyStep extends ConfigurableStep implements StepManyToMany<String, String> {
        @Override
        public Multi<String> applyTransform(Multi<String> input) {
            return input.onItem().transform(item -> item + "-mapped");
        }
    }
}
