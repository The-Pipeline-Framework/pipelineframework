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

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.StepConfig;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class PipelineRunnerTest {

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    PipelineRunner runner;

    @BeforeEach
    void setSequentialPolicy() {
        pipelineConfig.parallelism(ParallelismPolicy.SEQUENTIAL);
    }

    @Test
    void testPipelineRunnerCreation() {
        try (PipelineRunner injectedRunner = runner) {
            assertNotNull(injectedRunner);
        } catch (Exception e) {
            fail("PipelineRunner should be AutoCloseable");
        }
    }

    @Test
    void testRunWithStepOneToOne() {
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        List<Object> steps = List.of(new TestSteps.TestStepOneToOneBlocking());

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5)).assertCompleted();

        // Grab all items
        List<Object> actualItems = subscriber.getItems();

        // Expected items
        Set<String> expectedItems = Set.of("Processed: item1", "Processed: item2", "Processed: item3");

        // Assert ignoring order
        assertEquals(expectedItems, new HashSet<>(actualItems));

        // If duplicates matter, use sorted list instead:
        // List<String> expectedList = List.of(
        //    "Processed: item1",
        //    "Processed: item2",
        //    "Processed: item3"
        // );
        // assertEquals(expectedList.stream().sorted().toList(),
        //              actualItems.stream().map(Object::toString).sorted().tolist());
    }

    @Test
    void testRunWithStepOneToMany() {
        Multi<String> input = Multi.createFrom().items("item1", "item2");
        List<Object> steps = List.of(new TestSteps.TestStepOneToMany());

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(6));
        subscriber.awaitItems(6, Duration.ofSeconds(5));
        subscriber.assertItems("item1-1", "item1-2", "item1-3", "item2-1", "item2-2", "item2-3");
    }

    @Test
    void testRunWithStepManyToMany() {
        Multi<Object> input = Multi.createFrom().items("item1", "item2", "item3");
        List<Object> steps = List.of(new TestSteps.TestStepManyToMany());

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5));
        subscriber.assertItems("Streamed: item1", "Streamed: item2", "Streamed: item3");
    }

    @Test
    void testRunWithStepOneToAsync() {
        Multi<String> input = Multi.createFrom().items("item1", "item2", "item3");
        List<Object> steps = List.of(new TestSteps.TestStepOneToOne());

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5));
        subscriber.assertItems("Async: item1", "Async: item2", "Async: item3");
    }

    @Test
    void testRunWithMultipleSteps() {
        Multi<String> input = Multi.createFrom().items("item1", "item2");
        List<Object> steps = List.of(
                new TestSteps.TestStepOneToOneBlocking(),
                new TestSteps.TestStepOneToMany());

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(6));
        subscriber.awaitItems(6, Duration.ofSeconds(5)).assertCompleted();

        // Grab all items
        List<Object> actualItems = subscriber.getItems();

        // Expected items
        Set<String> expectedItems = Set.of(
                "Processed: item1-1",
                "Processed: item1-2",
                "Processed: item1-3",
                "Processed: item2-1",
                "Processed: item2-2",
                "Processed: item2-3");

        // Assert ignoring order
        assertEquals(expectedItems, new HashSet<>(actualItems));
    }

    @Test
    void testRunWithFailingStepAndRecovery() {
        Multi<String> input = Multi.createFrom().items("item1", "item2");
        List<Object> steps = List.of(new TestSteps.FailingStepBlocking(true));

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(30)).assertCompleted();

        // With recovery enabled, items should pass through unchanged
        // Order may vary due to asynchronous processing
        List<Object> actualItems = subscriber.getItems();

        // compare ignoring order
        assertEquals(
                Set.of("item1", "item2"), // expected as a set
                new HashSet<>(actualItems) // actual as a set
        );
    }

    @Test
    void testRunWithFailingStepWithoutRecovery() {
        Multi<String> input = Multi.createFrom().items("item1", "item2");
        List<Object> steps = List.of(new TestSteps.FailingStepBlocking());

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(1));
        subscriber.awaitFailure(Duration.ofSeconds(15));

        // Without recovery, should fail
        assertInstanceOf(RuntimeException.class, subscriber.getFailure());
        assertEquals("Intentional failure for testing", subscriber.getFailure().getMessage());
    }

    @Test
    void testRetryMechanismWithSuccess() {
        Multi<String> input = Multi.createFrom().items("item1");
        RetryTestSteps.AsyncFailNTimesStep step = new RetryTestSteps.AsyncFailNTimesStep(2);

        // Configure retry settings by initializing the step with proper configuration
        StepConfig stepConfig = new StepConfig();
        stepConfig.retryLimit(3).retryWait(Duration.ofMillis(10));
        step.initialiseWithConfig(stepConfig);

        Multi<Object> result = (Multi<Object>) runner.run(input, List.of((Object) step));

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(1));
        subscriber.awaitItems(1, Duration.ofSeconds(5));

        // Should succeed after 2 failures and 1 success
        subscriber.assertItems("Async Success: item1");
        assertEquals(3, step.getCallCount()); // 2 failures + 1 success
    }

    @Test
    void testRetryMechanismWithFailure() {
        Multi<String> input = Multi.createFrom().items("item1");
        RetryTestSteps.AsyncFailNTimesStep step = new RetryTestSteps.AsyncFailNTimesStep(3);

        // Configure retry settings - only 2 retries, but need 3 failures to pass
        StepConfig stepConfig = new StepConfig();
        stepConfig.retryLimit(2).retryWait(Duration.ofMillis(10));
        step.initialiseWithConfig(stepConfig);

        Multi<Object> result = (Multi<Object>) runner.run(input, List.of((Object) step));

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(1));
        subscriber.awaitFailure(Duration.ofSeconds(5));

        // Should fail after 2 retries (3 total attempts)
        assertInstanceOf(RuntimeException.class, subscriber.getFailure());
        assertEquals("Intentional async failure #3", subscriber.getFailure().getMessage());
        assertEquals(3, step.getCallCount()); // 2 retries + 1 initial = 3 total calls
    }

    @Test
    void testRetryWithRecovery() {
        Multi<String> input = Multi.createFrom().items("item1");
        RetryTestSteps.AsyncFailNTimesStep step = new RetryTestSteps.AsyncFailNTimesStep(3);

        // Configure recovery and retry settings
        StepConfig stepConfig = new StepConfig();
        stepConfig.recoverOnFailure(true).retryLimit(2).retryWait(Duration.ofMillis(10));
        step.initialiseWithConfig(stepConfig);

        Multi<Object> result = (Multi<Object>) runner.run(input, List.of((Object) step));

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(1));
        subscriber.awaitItems(1, Duration.ofSeconds(5));

        // Should recover after 2 retries (3 total attempts)
        subscriber.assertItems("item1"); // Original item passed through on recovery
        assertEquals(3, step.getCallCount()); // 2 retries + 1 initial = 3 total calls
    }

    @Test
    void testConfigurationIntegration() {
        Multi<String> input = Multi.createFrom().items("item1");

        // Create a step and configure it properly through the pipeline runner
        TestSteps.TestStepOneToOneBlocking step = new TestSteps.TestStepOneToOneBlocking();

        // Configure the step before running it through the pipeline
        StepConfig stepConfig = new StepConfig();
        stepConfig.retryLimit(5).retryWait(Duration.ofMillis(100));
        step.initialiseWithConfig(stepConfig);

        List<Object> steps = List.of(step);

        Multi<Object> result = (Multi<Object>) runner.run(input, steps);

        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(1));
        subscriber.awaitItems(1, Duration.ofSeconds(5)).assertCompleted();

        // Verify the configuration was applied
        assertEquals(5, step.retryLimit());
        assertEquals(Duration.ofMillis(100), step.retryWait());
    }

    @Test
    void testPipelineOrdering() throws Exception {
        // Create test steps in a specific order
        TestSteps.TestStepOneToOneBlocking stepB = new TestSteps.TestStepOneToOneBlocking();
        TestSteps.TestStepOneToMany stepA = new TestSteps.TestStepOneToMany();
        TestSteps.TestStepManyToMany stepC = new TestSteps.TestStepManyToMany();

        List<Object> originalSteps = List.of(stepB, stepA, stepC);

        // Access the orderSteps method using reflection to test the internal functionality
        java.lang.reflect.Method orderStepsMethod = PipelineRunner.class.getDeclaredMethod("orderSteps", List.class);
        orderStepsMethod.setAccessible(true);

        // When no pipeline order is configured, should return original order
        List<Object> orderedSteps = (List<Object>) orderStepsMethod.invoke(runner, originalSteps);

        // Verify the original order is preserved (no global configuration applied in this test)
        assertEquals(originalSteps.size(), orderedSteps.size());

        // Check that the same objects are present (just potentially reordered)
        assertEquals(originalSteps, orderedSteps);
    }
}
