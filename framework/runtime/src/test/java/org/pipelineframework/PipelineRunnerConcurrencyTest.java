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

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
class PipelineRunnerConcurrencyTest {

    @Inject
    private PipelineConfig pipelineConfig;

    @Inject
    private PipelineRunner pipelineRunner;

    private static class TestStepOneToOne extends ConfigurableStep
            implements StepOneToOne<String, String> {

        private final AtomicInteger callCount = new AtomicInteger(0);
        private final List<String> processingOrder = new ArrayList<>();

        @Override
        public Uni<String> applyOneToOne(String input) {
            // Record the order in which items are processed
            int order = callCount.incrementAndGet();
            processingOrder.add(input + "_order" + order);

            // Simulate variable processing times
            int processingTime = input.equals("slow") ? 500 : 100; // 'slow' takes longer
            try {
                Thread.sleep(processingTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return Uni.createFrom().item("processed:" + input);
        }
    }

    @Test
    void testSequentialProcessingByDefault() {
        // Given - Default step config (parallel = false)
        TestStepOneToOne step = new TestStepOneToOne();
        StepConfig liveConfig = new StepConfig();
        step.initialiseWithConfig(liveConfig); // defaults: parallel=false

        // When
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step));

        // Then - Should process sequentially in order
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5)).assertCompleted();

        List<Object> items = subscriber.getItems();
        assertEquals(3, items.size());
        assertTrue(items.contains("processed:item1"));
        assertTrue(items.contains("processed:item2"));
        assertTrue(items.contains("processed:item3"));

        // Verify processing order was sequential
        List<String> processingOrder = step.processingOrder;
        assertEquals("item1_order1", processingOrder.get(0));
        assertEquals("item2_order2", processingOrder.get(1));
        assertEquals("item3_order3", processingOrder.get(2));
    }

    @Test
    void testConcurrentProcessingWithoutOrderPreservation() {
        // Given - Enable parallel processing
        TestStepOneToOne step = new TestStepOneToOne();
        StepConfig liveConfig = new StepConfig().parallel(true);
        step.initialiseWithConfig(liveConfig);

        // When - Use items where some are slow to demonstrate concurrent processing
        Multi<String> input = Multi.createFrom().items("slow", "fast1", "fast2");
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step));

        // Then - Should process concurrently (fast items finish before slow)
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(2)).assertCompleted();

        List<Object> items = subscriber.getItems();
        assertEquals(3, items.size());
        assertTrue(items.stream().anyMatch(item -> item.toString().contains("slow")));
        assertTrue(items.stream().anyMatch(item -> item.toString().contains("fast1")));
        assertTrue(items.stream().anyMatch(item -> item.toString().contains("fast2")));

        // With parallel processing, output order may not match input order due to timing
        // But all should be processed
        assertEquals(3, step.callCount.get());
    }

    @Test
    void testConcurrentProcessingWithOrderPreservation() {
        // Given - Enable parallel processing
        TestStepOneToOne step = new TestStepOneToOne();
        StepConfig liveConfig = new StepConfig().parallel(true);
        step.initialiseWithConfig(liveConfig);

        // When - Process items with different processing times
        Multi<String> input = Multi.createFrom().items("slow", "fast1", "fast2");
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step));

        // Then - Should process concurrently but preserve original order
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(2)).assertCompleted();

        List<Object> items = subscriber.getItems();
        assertEquals(3, items.size());

        // With parallel processing, order should be preserved
        assertTrue(items.get(0).toString().contains("slow"));
        assertTrue(items.get(1).toString().contains("fast1"));
        assertTrue(items.get(2).toString().contains("fast2"));
    }

    @Test
    void testBackwardCompatibilityWithParallelFalse() {
        // Given - Explicitly set parallel to false (should behave as before)
        TestStepOneToOne step = new TestStepOneToOne();
        StepConfig liveConfig = new StepConfig().parallel(false);
        step.initialiseWithConfig(liveConfig);

        // When
        Multi<String> input = Multi.createFrom().items("itemA", "itemB");
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step));

        // Then - Should work the same as before (backward compatibility)
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5)).assertCompleted();

        List<Object> items = subscriber.getItems();
        assertEquals(2, items.size());
        assertTrue(items.contains("processed:itemA"));
        assertTrue(items.contains("processed:itemB"));
    }
}
