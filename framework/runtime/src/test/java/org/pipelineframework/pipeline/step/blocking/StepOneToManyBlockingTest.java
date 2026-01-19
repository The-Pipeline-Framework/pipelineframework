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

import java.time.Duration;
import java.util.List;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.blocking.StepOneToManyBlocking;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StepOneToManyBlockingTest {

    static class TestStepBlocking implements StepOneToManyBlocking<String, String> {
        @Override
        public List<String> applyList(String input) {
            return List.of(input + "-1", input + "-2", input + "-3");
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
    void testApplyListMethod() {
        // Given
        TestStepBlocking step = new TestStepBlocking();

        // When
        List<String> result = step.applyList("test");

        // Then
        assertEquals(List.of("test-1", "test-2", "test-3"), result);
    }

    @Test
    void testApplyMethodInStep() {
        // Given
        TestStepBlocking step = new TestStepBlocking();
        Multi<String> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<String> result = input.onItem()
                .transformToMulti(item -> Multi.createFrom().iterable(step.applyList(item)))
                .concatenate();

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(6));
        subscriber.awaitItems(6, Duration.ofSeconds(5));
        subscriber.assertItems("item1-1", "item1-2", "item1-3", "item2-1", "item2-2", "item2-3");
    }
}
