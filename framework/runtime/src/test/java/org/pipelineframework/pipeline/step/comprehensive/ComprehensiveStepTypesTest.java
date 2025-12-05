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

package org.pipelineframework.pipeline.step.comprehensive;

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
import org.pipelineframework.pipeline.step.collection.example.ExpandPaymentCollectionStep;
import org.pipelineframework.pipeline.step.future.example.ProcessPaymentFutureStep;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.blocking.StepOneToOneBlocking;

@QuarkusTest
public class ComprehensiveStepTypesTest {

    @Inject
    PipelineRunner pipelineRunner;

    @Test
    void testAllStepTypesWorkTogether() {
        // Given: Create test data
        Multi<String> input = Multi.createFrom().items("Payment1", "Payment2");

        // Create different types of steps and configure them properly
        ValidatePaymentStepBlocking validateStep = new ValidatePaymentStepBlocking();
        StepConfig validateConfig = new StepConfig();
        validateStep.initialiseWithConfig(validateConfig);

        ExpandPaymentCollectionStep expandStep = new ExpandPaymentCollectionStep();
        StepConfig expandConfig = new StepConfig();
        expandStep.initialiseWithConfig(expandConfig);

        ProcessPaymentFutureStep processStep = new ProcessPaymentFutureStep();
        StepConfig processConfig = new StepConfig();
        processStep.initialiseWithConfig(processConfig);

        // When: Run pipeline with mixed step types
        Multi<String> result = (Multi<String>) pipelineRunner.run(input, List.of(validateStep, expandStep, processStep));
        AssertSubscriber<String> subscriber = result.subscribe()
                .withSubscriber(AssertSubscriber.create(6)); // 2 inputs * 3 expanded each

        // Then: Verify results
        subscriber.awaitItems(6).awaitCompletion(Duration.ofSeconds(10));

        List<String> results = subscriber.getItems();
        assertEquals(6, results.size());

        // Verify all items were processed
        for (String res : results) {
            assertTrue(res.startsWith("Processed: TXN-"));
        }
    }

    // Standard StepOneToOneBlocking with blocking operations
    public static class ValidatePaymentStepBlocking extends ConfigurableStep
            implements StepOneToOneBlocking<String, String> {

        @Override
        public io.smallrye.mutiny.Uni<String> apply(String payment) {
            // Simulate validation work
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return io.smallrye.mutiny.Uni.createFrom().item("Validated: " + payment);
        }

        @Override
        public io.smallrye.mutiny.Uni<String> applyOneToOne(String input) {
            return apply(input);
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            super.initialiseWithConfig(config);
        }
    }
}
