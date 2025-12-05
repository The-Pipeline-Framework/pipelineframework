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

package org.pipelineframework.pipeline.step.future;

import static org.junit.jupiter.api.Assertions.*;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;

class StepOneToOneCompletableFutureTest {

    static class TestStepFuture implements StepOneToOneCompletableFuture<String, String> {
        @Override
        public CompletableFuture<String> applyAsync(String input) {
            return CompletableFuture.completedFuture("Future processed: " + input);
        }

        @Override
        public StepConfig effectiveConfig() {
            return new StepConfig();
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Use the config provided
        }
    }

    @Test
    void testApplyAsyncMethod() {
        // Given
        TestStepFuture step = new TestStepFuture();

        // When
        CompletableFuture<String> result = step.applyAsync("test");

        // Then
        String value = result.join();
        assertEquals("Future processed: test", value);
    }

    @Test
    void testDefaultParallel() {
        // Given
        TestStepFuture step = new TestStepFuture();

        // When
        boolean parallel = step.parallel();

        // Then
        assertFalse(parallel);
    }

    @Test
    void testApplyMethodInStep() {
        // Given
        TestStepFuture step = new TestStepFuture();
        Multi<String> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<String> result = input.onItem()
                .transformToUniAndConcatenate(
                        item -> step.apply(Uni.createFrom().item(item)));

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        subscriber.assertItems("Future processed: item1", "Future processed: item2");
    }
}
