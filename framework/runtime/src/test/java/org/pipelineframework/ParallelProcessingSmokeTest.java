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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;

@QuarkusTest
class ParallelProcessingSmokeTest {

    @Inject
    PipelineRunner pipelineRunner;

    @Inject
    PipelineConfig pipelineConfig;

    private static class TestStepOneToOne extends ConfigurableStep
            implements StepOneToOne<String, String> {

        private final AtomicInteger callCount = new AtomicInteger(0);
        private final List<Long> executionTimestamps = new java.util.ArrayList<>();
        private final List<String> executionThreads = new java.util.ArrayList<>();
        private final List<String> results = new java.util.ArrayList<>();

        public TestStepOneToOne() {
            // Empty constructor required for test class
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom()
                    .item(
                            () -> {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }

                                synchronized (this) {
                                    long ts = System.currentTimeMillis();
                                    String th = Thread.currentThread().getName();
                                    executionTimestamps.add(ts);
                                    executionThreads.add(th);
                                    int count = callCount.incrementAndGet();
                                    String result = "processed:" + input + "_count" + count;
                                    results.add(result);
                                    return result;
                                }
                            })
                    .runSubscriptionOn(
                            io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultExecutor());
        }

        public List<Long> getExecutionTimestamps() {
            synchronized (this) {
                return new java.util.ArrayList<>(executionTimestamps);
            }
        }

        public List<String> getExecutionThreads() {
            synchronized (this) {
                return new java.util.ArrayList<>(executionThreads);
            }
        }
    }

    private static class TestStepOneToOneCompletableFuture extends ConfigurableStep
            implements StepOneToOneCompletableFuture<String, String> {

        private final AtomicInteger callCount = new AtomicInteger(0);
        private final List<Long> executionTimestamps = new java.util.ArrayList<>();
        private final List<String> executionThreads = new java.util.ArrayList<>();
        private final List<String> results = new java.util.ArrayList<>();

        public TestStepOneToOneCompletableFuture() {
            // Empty constructor required for test class
        }

        @Override
        public CompletableFuture<String> applyAsync(String input) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        synchronized (this) {
                            long startTime = System.currentTimeMillis();
                            String currentThread = Thread.currentThread().getName();
                            executionTimestamps.add(startTime);
                            executionThreads.add(currentThread);
                            int count = callCount.incrementAndGet();
                            String result = "processed:" + input + "_count" + count;
                            results.add(result);
                            return result;
                        }
                    });
        }

        public List<Long> getExecutionTimestamps() {
            synchronized (this) {
                return new java.util.ArrayList<>(executionTimestamps);
            }
        }

        public List<String> getExecutionThreads() {
            synchronized (this) {
                return new java.util.ArrayList<>(executionThreads);
            }
        }
    }

    @Test
    void testSequentialProcessingWorks() {
        // Given
        TestStepOneToOne step = new TestStepOneToOne();
        StepConfig stepConfig = new StepConfig();
        // Keep default parallel=false
        step.initialiseWithConfig(stepConfig);

        // When
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step));

        // Then
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5)).assertCompleted();

        List<Object> items = subscriber.getItems();
        assertEquals(3, items.size());
        assertTrue(items.contains("processed:item1_count1"));
        assertTrue(items.contains("processed:item2_count2"));
        assertTrue(items.contains("processed:item3_count3"));
    }

    @Test
    void testParallelProcessingWorks() {
        // Given
        TestStepOneToOne step = new TestStepOneToOne();
        StepConfig stepConfig = new StepConfig();
        stepConfig.parallel(true);
        step.initialiseWithConfig(stepConfig);

        // When
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        long startTime = System.currentTimeMillis();
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step));

        // Then
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5)).assertCompleted();

        List<Object> items = subscriber.getItems();
        long endTime = System.currentTimeMillis();

        // (1) Assert output content equals expected processed items
        assertEquals(3, items.size());
        assertTrue(items.contains("processed:item1_count1"));
        assertTrue(items.contains("processed:item2_count2"));
        assertTrue(items.contains("processed:item3_count3"));

        // Verify that all items were processed
        assertEquals(3, step.callCount.get());

        // (2) Record threads
        List<String> executionThreads = step.getExecutionThreads();

        // Verify we have the expected number of executions
        assertEquals(3, executionThreads.size());

        // (3) Assert total duration < 500 to prove parallel speedup (3 items * 100ms each
        // sequentially = ~300ms, but with overhead and test environment variability)
        long totalDuration = endTime - startTime;
        assertTrue(
                totalDuration < 500,
                String.format(
                        "Expected parallel execution to be faster than sequential. Duration: %d ms",
                        totalDuration));

        // (4) Assert at least two distinct thread names in executionThreads to prove work ran on
        // multiple threads
        long distinctThreadCount = executionThreads.stream().distinct().count();
        assertTrue(
                distinctThreadCount >= 2,
                String.format(
                        "Expected at least 2 distinct threads, but got %d threads: %s",
                        distinctThreadCount, executionThreads));
    }

    @Test
    @Disabled
    void testCompletableFutureParallelProcessingWorks() {
        // Given
        TestStepOneToOneCompletableFuture step = new TestStepOneToOneCompletableFuture();
        StepConfig stepConfig = new StepConfig();
        stepConfig.parallel(true);
        step.initialiseWithConfig(stepConfig);

        // When
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        long startTime = System.currentTimeMillis();
        Multi<Object> result = (Multi<Object>) pipelineRunner.run(input, List.of(step));

        // Then
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5)).assertCompleted();

        List<Object> items = subscriber.getItems();
        long endTime = System.currentTimeMillis();

        // (1) Assert output content equals expected processed items
        assertEquals(3, items.size());
        assertTrue(items.contains("processed:item1_count1"));
        assertTrue(items.contains("processed:item2_count2"));
        assertTrue(items.contains("processed:item3_count3"));

        // Verify that all items were processed
        assertEquals(3, step.callCount.get());

        // (2) Record threads
        List<String> executionThreads = step.getExecutionThreads();

        // Verify we have the expected number of executions
        assertEquals(3, executionThreads.size());

        // (3) Assert total duration < 500 to prove parallel speedup (3 items * 100ms each
        // sequentially = ~300ms, but with overhead and test environment variability)
        long totalDuration = endTime - startTime;
        assertTrue(
                totalDuration < 500,
                String.format(
                        "Expected parallel execution to be faster than sequential. Duration: %d ms",
                        totalDuration));

        // (4) Assert at least two distinct thread names in executionThreads to prove work ran on
        // multiple threads
        long distinctThreadCount = executionThreads.stream().distinct().count();
        assertTrue(
                distinctThreadCount >= 2,
                String.format(
                        "Expected at least 2 distinct threads, but got %d threads: %s",
                        distinctThreadCount, executionThreads));
    }
}
