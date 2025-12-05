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

import static org.junit.jupiter.api.Assertions.*;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.StepManyToOne;

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

    static class ConfiguredTestStep implements StepManyToOne<String, String> {
        private StepConfig config = new StepConfig();

        public ConfiguredTestStep withParallel(boolean parallel) {
            this.config.parallel(parallel);
            return this;
        }

        @Override
        public Uni<String> applyReduce(Multi<String> input) {
            return input.collect()
                    .asList()
                    .onItem()
                    .transform(list -> "Reduced: " + String.join(", ", list));
        }

        @Override
        public StepConfig effectiveConfig() {
            return config;
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Use the config provided
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
    void testApplyMethodWithParallelConfig() {
        // Given
        ConfiguredTestStep step = new ConfiguredTestStep().withParallel(true);
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3", "item4", "item5", "item6");

        // When
        Uni<String> result = step.apply(input);

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem(java.time.Duration.ofSeconds(5));
        // All items processed and reduced in one operation
        subscriber.assertItem("Reduced: item1, item2, item3, item4, item5, item6");
    }

    @Test
    void testApplyMethodWithSequentialConfig() {
        // Given
        ConfiguredTestStep step = new ConfiguredTestStep().withParallel(false);
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");

        // When
        Uni<String> result = step.apply(input);

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem(java.time.Duration.ofSeconds(5));
        // All items processed and reduced in one operation
        subscriber.assertItem("Reduced: item1, item2, item3");
    }

    @Test
    void testApplyMethodUsesConfiguredValues() {
        // Given - Test that configured values take effect
        ConfiguredTestStep step = new ConfiguredTestStep().withParallel(false);
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3", "item4", "item5", "item6");

        // When
        Uni<String> result = step.apply(input);

        // Then - Should use configured parallel value (false)
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem(java.time.Duration.ofSeconds(5));
        // All items are reduced to a single output
        subscriber.assertItem("Reduced: item1, item2, item3, item4, item5, item6");
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
        ConfiguredTestStep step = new ConfiguredTestStep().withParallel(false);
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

    // Tests for deadLetterStream functionality
    @Test
    void testDeadLetterStreamWithEmptyStream() {
        // Given
        TestStep step = new TestStep();
        Multi<String> emptyStream = Multi.createFrom().empty();

        // When
        Uni<Void> result = step.deadLetterStream(emptyStream, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testDeadLetterStreamWithSingleItem() {
        // Given
        TestStep step = new TestStep();
        Multi<String> singleItemStream = Multi.createFrom().item("single");

        // When
        Uni<Void> result = step.deadLetterStream(singleItemStream, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testDeadLetterStreamWithMultipleItems() {
        // Given
        TestStep step = new TestStep();
        java.util.List<String> items = java.util.List.of("item1", "item2", "item3");
        Multi<String> multiStream = Multi.createFrom().iterable(items);

        // When
        Uni<Void> result = step.deadLetterStream(multiStream, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testDeadLetterStreamWithMoreItemsThanSampleSize() {
        // Given
        TestStep step = new TestStep();
        Multi<String> multiStream = Multi.createFrom()
                .items("item1", "item2", "item3", "item4", "item5", "item6", "item7");

        // When
        Uni<Void> result = step.deadLetterStream(multiStream, new RuntimeException("Test error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void testDeadLetterStreamWithErrorInStream() {
        // Given
        TestStep step = new TestStep();
        RuntimeException streamError = new RuntimeException("Stream error");
        Multi<String> errorStream = Multi.createFrom().failure(streamError);

        // When
        Uni<Void> result = step.deadLetterStream(errorStream, new RuntimeException("Processing error"))
                .replaceWithVoid();

        // Then
        io.smallrye.mutiny.helpers.test.UniAssertSubscriber<Void> subscriber = result.subscribe()
                .withSubscriber(
                        io.smallrye.mutiny.helpers.test.UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }
}
