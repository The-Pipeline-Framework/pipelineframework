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

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.blocking.StepOneToOneBlocking;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StepOneToOneBlockingTest {

    static class TestStepBlocking implements StepOneToOneBlocking<String, String> {
        @Override
        public Uni<String> apply(String input) {
            // This is a blocking operation that simulates processing
            try {
                Thread.sleep(10); // Simulate some work
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Uni.createFrom().item("Processed: " + input);
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return apply(input);
        }

        @Override
        public org.pipelineframework.config.StepConfig effectiveConfig() {
            return new org.pipelineframework.config.StepConfig();
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Use the config provided
        }
    }

    @Test
    void testApplyMethod() {
        // Given
        TestStepBlocking step = new TestStepBlocking();

        // When
        Uni<String> uniResult = step.apply("test");
        String result = uniResult.await().indefinitely();

        // Then
        assertEquals("Processed: test", result);
    }

    @Test
    void testApplyMethodInStep() {
        // Given
        TestStepBlocking step = new TestStepBlocking();
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");

        // When
        Multi<String> result = input.onItem().transformToUni(step::apply).concatenate();

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5));

        // Grab all items
        List<String> actualItems = subscriber.getItems();

        // Expected items
        Set<String> expectedItems = Set.of("Processed: item1", "Processed: item2", "Processed: item3");

        // Assert ignoring order
        assertEquals(expectedItems, new HashSet<>(actualItems));
    }
}
