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
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class DataFlowVerificationTest {

    @Inject
    private PipelineRunner pipelineRunner;

    @Inject
    private PipelineConfig pipelineConfig;

    // Simple step that tracks how many items it processes
    private static class TrackingStep extends ConfigurableStep
            implements StepOneToOne<String, String> {

        private final AtomicInteger processedCount = new AtomicInteger(0);
        private final String name;

        TrackingStep(String name) {
            this.name = name;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            int count = processedCount.incrementAndGet();
            System.out.println(name + " processing item #" + count + ": " + input);
            return Uni.createFrom().item("processed:" + input + "_by_" + name);
        }

        public int getProcessedCount() {
            return processedCount.get();
        }
    }

    @Test
    void testDataFlowIntegrity() {
        System.out.println("=== Testing Data Flow Integrity ===");
        pipelineConfig.parallelism(ParallelismPolicy.SEQUENTIAL);

        // Create 3 input items
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        System.out.println("Created input with 3 items: item1, item2, item3");

        // Create two tracking steps
        TrackingStep step1 = new TrackingStep("Step1");
        TrackingStep step2 = new TrackingStep("Step2");

        // Initialize steps with proper configuration
        StepConfig config1 = new StepConfig();
        step1.initialiseWithConfig(config1);

        StepConfig config2 = new StepConfig();
        step2.initialiseWithConfig(config2);

        // Run pipeline: input -> step1 -> step2
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step1, step2));

        // Subscribe and collect results
        AssertSubscriber<Object> subscriber = result.subscribe()
                .withSubscriber(AssertSubscriber.create(10)); // Buffer more than needed

        System.out.println("Waiting for results...");
        subscriber.awaitItems(3, Duration.ofSeconds(10)).assertCompleted();

        List<Object> items = subscriber.getItems();
        System.out.println("Received " + items.size() + " items:");
        for (int i = 0; i < items.size(); i++) {
            System.out.println("  Item " + (i + 1) + ": " + items.get(i));
        }

        // Verify all items were processed
        assertEquals(3, items.size(), "Should receive 3 items");

        // Check that all expected items are present (order may vary)
        List<String> itemStrings = items.stream().map(Object::toString).toList();
        assertTrue(
                itemStrings.contains("processed:processed:item1_by_Step1_by_Step2"),
                "Should contain item1 result");
        assertTrue(
                itemStrings.contains("processed:processed:item2_by_Step1_by_Step2"),
                "Should contain item2 result");
        assertTrue(
                itemStrings.contains("processed:processed:item3_by_Step1_by_Step2"),
                "Should contain item3 result");

        // Verify both steps processed all items
        assertEquals(3, step1.getProcessedCount(), "Step 1 should process 3 items");
        assertEquals(3, step2.getProcessedCount(), "Step 2 should process 3 items");

        System.out.println("=== Test completed successfully ===");
    }

    @Test
    void testDataFlowIntegrityWithParallelProcessing() {
        System.out.println("=== Testing Data Flow Integrity with Parallel Processing ===");
        pipelineConfig.parallelism(ParallelismPolicy.PARALLEL);

        // Create 3 input items
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        System.out.println("Created input with 3 items: item1, item2, item3");

        // Create two tracking steps with parallel processing enabled
        TrackingStep step1 = new TrackingStep("Step1_Parallel");
        TrackingStep step2 = new TrackingStep("Step2_Parallel");

        // Initialize steps
        StepConfig config1 = new StepConfig();
        step1.initialiseWithConfig(config1);

        StepConfig config2 = new StepConfig();
        step2.initialiseWithConfig(config2);

        // Run pipeline: input -> step1 -> step2
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step1, step2));

        // Subscribe and collect results
        AssertSubscriber<Object> subscriber = result.subscribe()
                .withSubscriber(AssertSubscriber.create(10)); // Buffer more than needed

        System.out.println("Waiting for results...");
        subscriber.awaitItems(3, Duration.ofSeconds(10)).assertCompleted();

        List<Object> items = subscriber.getItems();
        System.out.println("Received " + items.size() + " items:");
        for (int i = 0; i < items.size(); i++) {
            System.out.println("  Item " + (i + 1) + ": " + items.get(i));
        }

        // Verify all items were processed
        assertEquals(3, items.size(), "Should receive 3 items");

        // Check that all expected items are present (order may vary with parallel processing)
        List<String> itemStrings = items.stream().map(Object::toString).toList();
        assertTrue(
                itemStrings.contains(
                        "processed:processed:item1_by_Step1_Parallel_by_Step2_Parallel"),
                "Should contain item1 result");
        assertTrue(
                itemStrings.contains(
                        "processed:processed:item2_by_Step1_Parallel_by_Step2_Parallel"),
                "Should contain item2 result");
        assertTrue(
                itemStrings.contains(
                        "processed:processed:item3_by_Step1_Parallel_by_Step2_Parallel"),
                "Should contain item3 result");

        // Verify both steps processed all items
        assertEquals(3, step1.getProcessedCount(), "Step 1 should process 3 items");
        assertEquals(3, step2.getProcessedCount(), "Step 2 should process 3 items");

        System.out.println("=== Test completed successfully ===");
    }
}
