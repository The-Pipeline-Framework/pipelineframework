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
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.StepOneToOne;

class StepTest {

    static class TestStep implements StepOneToOne<Object, Object> {
        private StepConfig config = new StepConfig();

        @Override
        public StepConfig effectiveConfig() {
            return config;
        }

        @Override
        public Uni<Object> applyOneToOne(Object input) {
            return Uni.createFrom().item(input); // Pass through
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            this.config = config;
        }
    }

    @Test
    void testDefaultMethods() {
        // Given
        TestStep step = new TestStep();

        // When & Then
        assertEquals(3, step.retryLimit());
        assertEquals(Duration.ofMillis(2000), step.retryWait());
        assertFalse(step.recoverOnFailure());
        assertEquals(Duration.ofSeconds(30), step.maxBackoff());
        assertFalse(step.jitter());
    }

    @Test
    void testDeadLetterMethod() {
        // Given
        TestStep step = new TestStep();

        // When
        var result = step.deadLetter(
                io.smallrye.mutiny.Uni.createFrom().item("testItem"),
                new RuntimeException("test error"));

        // Then
        assertNotNull(result);
        // The deadLetter method should complete successfully
        result.await().indefinitely();
    }

    @Test
    void testApplyMethod() {
        // Given
        TestStep step = new TestStep();
        Multi<Object> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<Object> result = input.onItem().transformToUni(step::applyOneToOne).concatenate();

        // Then
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        subscriber.assertItems("item1", "item2");
    }
}
