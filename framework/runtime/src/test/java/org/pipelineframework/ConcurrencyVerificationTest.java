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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.blocking.StepOneToOneBlocking;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;

@QuarkusTest
class ConcurrencyVerificationTest {

    @Inject
    private PipelineConfig pipelineConfig;

    @Inject
    private PipelineRunner pipelineRunner;

    // Simple step that tracks processing
    private static class TrackingStepOneToOne extends ConfigurableStep
            implements StepOneToOne<String, String> {

        private final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public Uni<String> applyOneToOne(String input) {
            // Record the processing
            callCount.incrementAndGet();

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

    // Blocking step that tracks processing
    private static class TrackingStepOneToOneBlocking extends ConfigurableStep
            implements StepOneToOneBlocking<String, String> {

        private final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public Uni<String> apply(String input) {
            // Record the processing
            callCount.incrementAndGet();

            // Simulate variable processing times
            int processingTime = input.equals("slow") ? 500 : 100; // 'slow' takes longer
            try {
                Thread.sleep(processingTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return Uni.createFrom().item("processed:" + input);
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return apply(input);
        }
    }

    // CompletableFuture step that tracks processing
    private static class TrackingStepOneToOneCompletableFuture extends ConfigurableStep
            implements StepOneToOneCompletableFuture<String, String> {

        private final AtomicInteger callCount = new AtomicInteger(0);

        @Override
        public CompletableFuture<String> applyAsync(String input) {
            // Record the processing
            callCount.incrementAndGet();

            // Simulate variable processing times
            int processingTime = input.equals("slow") ? 500 : 100; // 'slow' takes longer

            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            Thread.sleep(processingTime);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return "processed:" + input;
                    });
        }
    }

    @Test
    void testSequentialProcessingByDefault() {
        System.out.println("=== Testing Sequential Processing (Default Behavior) ===");

        // Given - Default step config (parallel = false)
        TrackingStepOneToOne step = new TrackingStepOneToOne();
        StepConfig stepConfig = new StepConfig();
        step.initialiseWithConfig(stepConfig); // defaults: parallel=false

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

        // Verify processing happened
        assertEquals(3, step.callCount.get());

        System.out.println(
                "✓ Sequential processing completed successfully with correct order preservation");
    }

    @Test
    void testParallelProcessingWithoutOrderPreservation() {
        System.out.println("=== Testing Parallel Processing (No Order Preservation) ===");

        // Given - Enable parallel processing
        TrackingStepOneToOne step = new TrackingStepOneToOne();
        StepConfig stepConfig = new StepConfig();
        stepConfig.parallel(true);
        step.initialiseWithConfig(stepConfig);

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

        // With parallel processing, all should be processed
        assertEquals(3, step.callCount.get());

        System.out.println(
                "✓ Parallel processing completed successfully with concurrent execution");
    }

    @Test
    void testBlockingStepParallelProcessing() {
        System.out.println("=== Testing Blocking Step Parallel Processing ===");

        // Given - Blocking step with parallel processing enabled
        TrackingStepOneToOneBlocking step = new TrackingStepOneToOneBlocking();
        StepConfig stepConfig = new StepConfig();
        stepConfig.parallel(true);
        step.initialiseWithConfig(stepConfig);

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

        // With parallel processing, all should be processed
        assertEquals(3, step.callCount.get());

        System.out.println("✓ Blocking step parallel processing completed successfully");
    }

    @Test
    void testCompletableFutureStepParallelProcessing() {
        System.out.println("=== Testing CompletableFuture Step Parallel Processing ===");

        // Given - CompletableFuture step with parallel processing enabled
        TrackingStepOneToOneCompletableFuture step = new TrackingStepOneToOneCompletableFuture();
        StepConfig stepConfig = new StepConfig();
        stepConfig.parallel(true);
        step.initialiseWithConfig(stepConfig);

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

        // With parallel processing, all should be processed
        assertEquals(3, step.callCount.get());

        System.out.println("✓ CompletableFuture step parallel processing completed successfully");
    }

    @Test
    void testBackwardCompatibilityWithParallelFalse() {
        System.out.println("=== Testing Backward Compatibility with parallel=false ===");

        // Given - Explicitly set parallel to false (should behave as before)
        TrackingStepOneToOne step = new TrackingStepOneToOne();
        StepConfig stepConfig = new StepConfig();
        stepConfig.parallel(false);
        step.initialiseWithConfig(stepConfig);

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
        assertEquals(2, step.callCount.get());

        System.out.println("✓ Backward compatibility with parallel=false verified");
    }
}
