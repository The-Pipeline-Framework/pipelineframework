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
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.StepOneToMany;

class StepOneToManyTest {

    static class TestStep implements StepOneToMany<String, String> {
        @Override
        public Multi<String> applyOneToMany(String input) {
            return Multi.createFrom().items(input + "-1", input + "-2", input + "-3");
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
    void testApplyMultiMethod() {
        // Given
        TestStep step = new TestStep();

        // When
        Multi<String> result = step.applyOneToMany("test");

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5));
        subscriber.assertItems("test-1", "test-2", "test-3");
    }

    @Test
    void testApplyMethod() {
        // Given
        TestStep step = new TestStep();
        Multi<String> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<String> result = input.onItem()
                .transformToMulti(item -> step.apply(Uni.createFrom().item(item)))
                .concatenate();

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(6));
        subscriber.awaitItems(6, Duration.ofSeconds(5));
        subscriber.assertItems("item1-1", "item1-2", "item1-3", "item2-1", "item2-2", "item2-3");
    }
}
