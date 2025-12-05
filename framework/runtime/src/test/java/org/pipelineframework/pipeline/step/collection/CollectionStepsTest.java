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

package org.pipelineframework.pipeline.step.collection;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToMany;

@QuarkusTest
public class CollectionStepsTest {

    @Inject
    PipelineRunner pipelineRunner;

    @Test
    void testOneToManyCollectionStep() {
        // Given: Create test data
        Multi<String> input = Multi.createFrom().items("Payment1", "Payment2");

        // Create steps and configure them properly
        ValidatePaymentStepBlocking validateStep = new ValidatePaymentStepBlocking();
        // Configure step without calling liveConfig() directly
        StepConfig validateConfig = new StepConfig();
        validateStep.initialiseWithConfig(validateConfig);

        // Create ExpandPaymentCollectionStep using the same pattern
        ExpandPaymentCollectionStep expandStep = new ExpandPaymentCollectionStep();
        StepConfig expandConfig = new StepConfig();
        expandStep.initialiseWithConfig(expandConfig);

        // When: Run pipeline
        Multi<String> result = (Multi<String>) pipelineRunner.run(input, List.of(validateStep, expandStep));
        AssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(AssertSubscriber.create(6)); // 2 inputs * 3 expanded each

        // Then: Verify results
        subscriber.awaitItems(6).awaitCompletion(Duration.ofSeconds(10));

        List<String> results = subscriber.getItems();
        assertEquals(6, results.size());

        // Verify all items were processed
        assertTrue(results.contains("TXN-001-Validated: Payment1"));
        assertTrue(results.contains("TXN-002-Validated: Payment1"));
        assertTrue(results.contains("TXN-003-Validated: Payment1"));
        assertTrue(results.contains("TXN-001-Validated: Payment2"));
        assertTrue(results.contains("TXN-002-Validated: Payment2"));
        assertTrue(results.contains("TXN-003-Validated: Payment2"));
    }

    // Helper step for validating payments - this should be a OneToOne step
    public static class ValidatePaymentStepBlocking extends ConfigurableStep
            implements org.pipelineframework.step.blocking.StepOneToOneBlocking<String, String> {

        @Override
        public io.smallrye.mutiny.Uni<String> apply(String input) {
            // Apply validation to the input
            return io.smallrye.mutiny.Uni.createFrom().item("Validated: " + input);
        }

        @Override
        public io.smallrye.mutiny.Uni<String> applyOneToOne(String input) {
            // Apply validation to the input
            return apply(input);
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            super.initialiseWithConfig(config);
        }
    }

    // Helper step for expanding payments to multiple transactions - this should expand each input
    // to multiple outputs
    public static class ExpandPaymentCollectionStep extends ConfigurableStep
            implements StepOneToMany<String, String> {

        @Override
        public Multi<String> applyOneToMany(String input) {
            // For each input (like "Validated: Payment1"), create 3 transaction items
            return Multi.createFrom()
                    .items("TXN-001-" + input, "TXN-002-" + input, "TXN-003-" + input);
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            super.initialiseWithConfig(config);
        }
    }
}
