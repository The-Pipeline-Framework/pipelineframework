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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.blocking.StepManyToManyBlocking;

class StepManyToManyBlockingTest {

    static class TestStepBlocking implements StepManyToManyBlocking<Object, Object> {
        @Override
        public List<Object> applyStreamingList(List<Object> upstream) {
            List<Object> result = new ArrayList<>();
            for (Object item : upstream) {
                result.add("Streamed: " + item);
            }
            return result;
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
    void testApplyStreamingListMethod() {
        // Given
        TestStepBlocking step = new TestStepBlocking();
        List<Object> input = new ArrayList<>();
        input.add("item1");
        input.add("item2");

        // When
        List<Object> result = step.applyStreamingList(input);

        // Then
        assertEquals(2, result.size());
        assertEquals("Streamed: item1", result.get(0));
        assertEquals("Streamed: item2", result.get(1));
    }

    @Test
    void testApplyMethodInStep() {
        // Given
        TestStepBlocking step = new TestStepBlocking();
        Multi<Object> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<Object> result = step.apply(input);

        // Then
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        subscriber.assertItems("Streamed: item1", "Streamed: item2");
    }
}
