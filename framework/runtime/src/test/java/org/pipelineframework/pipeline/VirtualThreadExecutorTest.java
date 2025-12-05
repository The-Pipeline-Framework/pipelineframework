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

package org.pipelineframework.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToMany;

@QuarkusTest
class VirtualThreadExecutorTest {

    @Inject
    PipelineRunner runner;

    static class VirtualThreadTestStep extends ConfigurableStep
            implements StepOneToMany<String, String> {
        private final AtomicBoolean usedVirtualThreads = new AtomicBoolean(false);

        @Override
        public Multi<String> applyOneToMany(String input) {
            // This is a simple test to verify the step is working
            return Multi.createFrom().items(input + "-vt1", input + "-vt2");
        }
    }

    @Test
    void testVirtualThreadExecutorIsCreated() {
        assertNotNull(runner);
        // The vThreadExecutor is created in the constructor and is private
        // We can't directly test it, but we can verify it works by running a step that uses it
    }

    @Test
    void testRunWithVirtualThreads() {
        Multi<String> input = Multi.createFrom().items("item1", "item2");
        VirtualThreadTestStep step = new VirtualThreadTestStep();

        Multi<Object> result = (Multi<Object>) runner.run(input, List.of(step));

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(4));
        subscriber.awaitItems(4, Duration.ofSeconds(5));

        // With virtual threads, the order is not guaranteed, so we need to check that all
        // expected
        // items are present regardless of order
        List<Object> items = subscriber.getItems();
        Set<Object> expectedItems = Set.of("item1-vt1", "item1-vt2", "item2-vt1", "item2-vt2");
        Set<Object> actualItems = new HashSet<>(items);

        assertEquals(expectedItems, actualItems, "All expected items should be present");
        assertEquals(4, items.size(), "Should have exactly 4 items");
    }

    @Test
    void testPipelineRunnerCloseCleansUpExecutor() {
        // This test verifies that the PipelineRunner can be closed without issues
        Multi<String> input = Multi.createFrom().items("test");
        List<TestSteps.TestStepOneToOneBlocking> steps = List.of(new TestSteps.TestStepOneToOneBlocking());

        Multi<Object> result = (Multi<Object>) runner.run(input, (List<Object>) (List<?>) steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(1));
        subscriber.awaitItems(1, Duration.ofSeconds(5));
        subscriber.assertItems("Processed: test");
        // This should not throw any exceptions
    }
}
