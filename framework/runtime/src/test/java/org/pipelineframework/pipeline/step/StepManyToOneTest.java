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

package org.pipelineframework.pipeline.step;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.StepManyToOne;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class StepManyToOneTest {

    static class TestStep implements StepManyToOne<String, String> {
        @Override
        public Uni<String> applyReduce(Multi<String> input) {
            return input.collect()
                    .asList()
                    .onItem()
                    .transform(list -> "Reduced: " + String.join(", ", list));
        }

        @Override
        public org.pipelineframework.config.StepConfig effectiveConfig() {
            // Return an empty StepConfig so the method defaults are not overridden in the
            // apply method
            return new org.pipelineframework.config.StepConfig();
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Use the config provided
        }
    }

    static class FailingRecoverStep implements StepManyToOne<String, String> {
        private final AtomicBoolean rejectCalled = new AtomicBoolean(false);

        @Override
        public Uni<String> applyReduce(Multi<String> input) {
            return Uni.createFrom().failure(new RuntimeException("stream boom"));
        }

        @Override
        public StepConfig effectiveConfig() {
            return new StepConfig().recoverOnFailure(true).retryLimit(1);
        }

        @Override
        public void initialiseWithConfig(StepConfig config) {
            // no-op
        }

        @Override
        public Uni<String> rejectStream(
            java.util.List<String> sampleItems,
            long totalItemCount,
            Throwable cause,
            Integer retriesObserved,
            Integer retryLimit
        ) {
            rejectCalled.set(true);
            return Uni.createFrom().item("recovered-stream");
        }

        boolean rejectCalled() {
            return rejectCalled.get();
        }
    }

    @Test
    void testApplyReduceMethod() {
        // Given
        TestStep step = new TestStep();
        Multi<String> inputs = Multi.createFrom().items("item1", "item2", "item3");

        // When
        Uni<String> result = step.applyReduce(inputs);

        // Then
        String value = result.await().indefinitely();
        assertEquals("Reduced: item1, item2, item3", value);
    }

    @Test
    void testApplyMethod() {
        // Given
        TestStep step = new TestStep();
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3", "item4");

        // When
        Uni<String> result = step.apply(input);

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem(java.time.Duration.ofSeconds(5));
        // All items processed and reduced in one operation
        subscriber.assertItem("Reduced: item1, item2, item3, item4");
    }

    @Test
    void testDefaultStepConfigValues() {
        // Given
        TestStep step = new TestStep();
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3", "item4");

        // When
        Uni<String> result = step.apply(input);

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem(java.time.Duration.ofSeconds(5));
        // All 4 items are processed and reduced in one operation
        subscriber.assertItem("Reduced: item1, item2, item3, item4");
    }

    @Test
    void testCsvProcessingUseCase_Reduction() {
        // Given - Simulate the CSV processing scenario where we want to process and reduce all
        // related records
        TestStep step = new TestStep();
        Multi<String> input = Multi.createFrom()
                .range(1, 13) // 12 items simulating 12 PaymentOutput records
                .map(i -> "payment_" + i + "_for_csv_file_X");

        // When
        Uni<String> result = step.apply(input);

        // Then - All 12 items should be processed and reduced to a single result
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem(java.time.Duration.ofSeconds(5));
        // All items should be in one reduced result
        String resultString = subscriber.getItem();
        assertTrue(resultString.contains("payment_1_for_csv_file_X"));
        assertTrue(resultString.contains("payment_12_for_csv_file_X"));
        // The result should contain all 12 items
        assertTrue(
                resultString.contains(
                        "payment_1_for_csv_file_X, payment_2_for_csv_file_X, payment_3_for_csv_file_X"));
    }

    // Tests for rejectStream functionality
    @Test
    void testRejectStreamWithEmptyStream() {
        // Given
        TestStep step = new TestStep();
        java.util.List<String> emptyStream = java.util.List.of();

        // When
        Uni<Void> result = step.rejectStream(emptyStream, 0L, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testRejectStreamWithSingleItem() {
        // Given
        TestStep step = new TestStep();
        java.util.List<String> singleItemStream = java.util.List.of("single");

        // When
        Uni<Void> result = step.rejectStream(singleItemStream, 1L, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testRejectStreamWithMultipleItems() {
        // Given
        TestStep step = new TestStep();
        java.util.List<String> multiStream = java.util.List.of("item1", "item2", "item3");

        // When
        Uni<Void> result = step.rejectStream(multiStream, 3L, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testRejectStreamWithMoreItemsThanSampleSize() {
        // Given
        TestStep step = new TestStep();
        java.util.List<String> multiStream = java.util.List.of(
            "item1", "item2", "item3", "item4", "item5", "item6", "item7");

        // When
        Uni<Void> result = step.rejectStream(multiStream, 7L, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testRejectStreamWithErrorInStream() {
        // Given
        TestStep step = new TestStep();
        // When
        Uni<Void> result = step.rejectStream(java.util.List.of(), 0L, new RuntimeException("Processing error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void recoverOnFailureRoutesToRejectStreamAndContinues() {
        FailingRecoverStep step = new FailingRecoverStep();

        String result = step.apply(Multi.createFrom().items("a", "b")).await().indefinitely();

        assertEquals("recovered-stream", result);
        assertTrue(step.rejectCalled());
    }
}
