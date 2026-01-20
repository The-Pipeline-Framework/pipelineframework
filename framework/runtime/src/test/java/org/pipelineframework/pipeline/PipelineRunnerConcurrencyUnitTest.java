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

package org.pipelineframework.pipeline;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineRunnerConcurrencyUnitTest {

    static class TestConcurrentStep extends ConfigurableStep
            implements StepOneToOne<String, String> {

        private final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public Uni<String> applyOneToOne(String input) {
            // Increment call count to track how many items are being processed
            callCount.incrementAndGet();

            // Simulate processing time that varies by input using Uni.delay for async behavior
            Duration delay = input.equals("slow")
                    ? Duration.ofMillis(500)
                    : Duration.ofMillis(100); // slow item takes longer

            return Uni.createFrom().item("processed:" + input).onItem().delayIt().by(delay);
        }
    }

    @Test
    void testSequentialProcessingByDefault() {
        // Given
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");

        TestConcurrentStep step = new TestConcurrentStep();
        step.initialiseWithConfig(new org.pipelineframework.config.StepConfig());

        // When - Use the PipelineRunner's applyOneToOneUnchecked method directly
        Multi<String> result = (Multi<String>) PipelineRunnerTestHelper.applyOneToOne(step, input, false);

        // Then - Should process sequentially
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        List<String> items = subscriber.awaitItems(3, Duration.ofSeconds(5)).getItems();

        assertEquals(3, items.size());
        assertTrue(items.contains("processed:item1"));
        assertTrue(items.contains("processed:item2"));
        assertTrue(items.contains("processed:item3"));

        assertEquals(3, step.callCount.get());
    }

    @Test
    void testConcurrentProcessingWithoutOrderPreservation() {
        // Given
        Multi<String> input = Multi.createFrom().items("slow", "fast1", "fast2"); // slow item first

        TestConcurrentStep step = new TestConcurrentStep();
        step.initialiseWithConfig(new org.pipelineframework.config.StepConfig());

        // When
        Multi<String> result = (Multi<String>) PipelineRunnerTestHelper.applyOneToOne(step, input, true);

        // Then - Should process concurrently, with fast items potentially completing before slow
        // item
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        List<String> items = subscriber.awaitItems(3, Duration.ofSeconds(2)).getItems();

        assertEquals(3, items.size());
        assertTrue(items.contains("processed:slow"));
        assertTrue(items.contains("processed:fast1"));
        assertTrue(items.contains("processed:fast2"));

        // With MERGE strategy, output order may not match input order due to timing
        // But all should be processed
        assertEquals(3, step.callCount.get());
    }

    @Test
    void testBackwardCompatibilityWithParallelFalse() {
        // Given
        Multi<String> input = Multi.createFrom().items("itemA", "itemB");

        TestConcurrentStep step = new TestConcurrentStep();
        step.initialiseWithConfig(new org.pipelineframework.config.StepConfig());

        // When
        Multi<String> result = (Multi<String>) PipelineRunnerTestHelper.applyOneToOne(step, input, false);

        // Then - Should work the same as before (backward compatibility maintained)
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        List<String> items = subscriber.awaitItems(2, Duration.ofSeconds(5)).getItems();

        assertEquals(2, items.size());
        assertTrue(items.contains("processed:itemA"));
        assertTrue(items.contains("processed:itemB"));
        assertEquals(2, step.callCount.get());
    }

    // Helper class to access package-private methods for testing
    static class PipelineRunnerTestHelper {
        public static Object applyOneToOne(Object step, Object current, boolean parallel) {
            return org.pipelineframework.PipelineRunner.applyOneToOneUnchecked(
                    (org.pipelineframework.step.StepOneToOne<String, String>) step, current, parallel, 128, null, null);
        }
    }
}
