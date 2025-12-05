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

package org.pipelineframework.pipeline.step.blocking;

import static org.junit.jupiter.api.Assertions.*;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.blocking.StepManyToOneBlocking;

class StepManyToOneBlockingTest {

    static class TestStepBlocking implements StepManyToOneBlocking<String, String> {
        @Override
        public String applyBatchList(List<String> inputs) {
            return "Batch processed: " + String.join(", ", inputs);
        }

        @Override
        public StepConfig effectiveConfig() {
            return new StepConfig();
        }

        @Override
        public int batchSize() {
            return 2; // Small batch size for testing
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Use the config provided
        }
    }

    @Test
    void testApplyBatchListMethod() {
        // Given
        TestStepBlocking step = new TestStepBlocking();
        List<String> inputs = List.of("item1", "item2", "item3");

        // When
        String result = step.applyBatchList(inputs);

        // Then
        assertEquals("Batch processed: item1, item2, item3", result);
    }

    @Test
    void testDefaultBatchSize() {
        // Given
        TestStepBlocking step = new TestStepBlocking();

        // When
        int batchSize = step.batchSize();

        // Then
        assertEquals(2, batchSize); // Our overridden value
    }

    @Test
    void testDefaultBatchTimeoutMs() {
        // Given
        TestStepBlocking step = new TestStepBlocking();

        // When
        Duration batchTimeout = step.batchTimeout();

        // Then
        assertEquals(Duration.ofMillis(1000), batchTimeout);
    }

    @Test
    void testApplyMethodInStep() {
        // Given
        TestStepBlocking step = new TestStepBlocking();
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3", "item4");

        // When
        Multi<String> result = input.group()
                .intoLists()
                .of(step.batchSize())
                .onItem()
                .transformToUniAndMerge(
                        list -> io.smallrye.mutiny.Uni.createFrom()
                                .item(step.applyBatchList(list)))
                .onItem()
                .transformToMulti(item -> io.smallrye.mutiny.Multi.createFrom().item(item))
                .concatenate();

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        subscriber.assertItems("Batch processed: item1, item2", "Batch processed: item3, item4");
    }
}
