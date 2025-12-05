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
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.StepSideEffect;

class StepSideEffectTest {

    private static final Logger LOG = Logger.getLogger(StepSideEffectTest.class);

    static class TestStep implements StepSideEffect<String> {
        @Override
        public Uni<String> applyOneToOne(String input) {
            // Simulate some side effect
            LOG.infof("Side effect for: %s", input);
            return Uni.createFrom().item(input); // Return original input after side effect
        }

        @Override
        public StepConfig effectiveConfig() {
            return new StepConfig();
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Use the passed config
        }
    }

    @Test
    void testApplyMethod() {
        // Given
        TestStep step = new TestStep();

        // When
        Uni<String> result = step.applyOneToOne("test");

        // Then
        String value = result.await().indefinitely();
        assertEquals("test", value);
    }

    @Test
    void testDefaultParallel() {
        // Given
        TestStep step = new TestStep();

        // When
        boolean parallel = step.parallel();

        // Then
        assertFalse(parallel);
    }

    @Test
    void testApplyMethodInStep() {
        // Given
        TestStep step = new TestStep();
        Multi<String> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<String> result = input.onItem().transformToUni(step::applyOneToOne).concatenate();

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        subscriber.assertItems("item1", "item2"); // Items should pass through unchanged
    }
}
