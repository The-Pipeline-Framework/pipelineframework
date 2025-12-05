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

package org.pipelineframework.pipeline.step.manytoone;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.blocking.StepOneToOneBlocking;
import org.pipelineframework.step.functional.ManyToOne;

@QuarkusTest
public class ManyToOneStepsTest {

    @Inject
    PipelineRunner pipelineRunner;

    @Test
    void testReactiveManyToOneStep() {
        // Given: Create test entities
        TestPaymentEntity payment1 = new TestPaymentEntity("John Doe", new BigDecimal("100.00"));
        TestPaymentEntity payment2 = new TestPaymentEntity("Jane Smith", new BigDecimal("200.50"));
        TestPaymentEntity payment3 = new TestPaymentEntity("Bob Johnson", new BigDecimal("75.25"));
        TestPaymentEntity payment4 = new TestPaymentEntity("Alice Brown", new BigDecimal("300.75"));

        Multi<TestPaymentEntity> input = Multi.createFrom().items(payment1, payment2, payment3, payment4);

        // Create steps and configure them properly
        ValidatePaymentStepBlocking validateStep = new ValidatePaymentStepBlocking();
        StepConfig validateConfig = new StepConfig();
        validateStep.initialiseWithConfig(validateConfig);

        PaymentAggregationStep aggregateStep = new PaymentAggregationStep();
        StepConfig aggregateConfig = new StepConfig();
        aggregateStep.initialiseWithConfig(aggregateConfig);

        // When: Run pipeline
        Object result1 = pipelineRunner.run(input, List.of(validateStep, aggregateStep));
        PaymentSummary result = ((io.smallrye.mutiny.Uni<PaymentSummary>) result1)
                .onItem()
                .castTo(PaymentSummary.class)
                .await()
                .atMost(Duration.ofSeconds(10));

        // Then: Verify result
        assertNotNull(result);
        assertEquals(4, result.getTotalPayments()); // All 4 payments
        assertEquals(
                new BigDecimal("676.50"),
                result.getTotalAmount()); // 100.00 + 200.50 + 75.25 + 300.75
    }

    @Test
    void testImperativeManyToOneStep() {
        // Given: Create test entities
        TestPaymentEntity payment1 = new TestPaymentEntity("John Doe", new BigDecimal("50.00"));
        TestPaymentEntity payment2 = new TestPaymentEntity("Jane Smith", new BigDecimal("150.25"));
        TestPaymentEntity payment3 = new TestPaymentEntity("Bob Johnson", new BigDecimal("25.50"));
        TestPaymentEntity payment4 = new TestPaymentEntity("Alice Brown", new BigDecimal("125.75"));
        TestPaymentEntity payment5 = new TestPaymentEntity("Charlie Wilson", new BigDecimal("80.00"));

        Multi<TestPaymentEntity> input = Multi.createFrom().items(payment1, payment2, payment3, payment4, payment5);

        // Create steps and configure them properly
        ValidatePaymentStepBlocking validateStep = new ValidatePaymentStepBlocking();
        StepConfig validateConfig = new StepConfig();
        validateStep.initialiseWithConfig(validateConfig);

        PaymentAggregationStepBlocking aggregateStep = new PaymentAggregationStepBlocking();
        StepConfig aggregateConfig = new StepConfig();
        aggregateStep.initialiseWithConfig(aggregateConfig);

        // When: Run pipeline
        Object result2 = pipelineRunner.run(input, List.of(validateStep, aggregateStep));
        PaymentSummary result = ((io.smallrye.mutiny.Uni<PaymentSummary>) result2)
                .onItem()
                .castTo(PaymentSummary.class)
                .await()
                .atMost(Duration.ofSeconds(10));

        // Then: Verify result
        assertNotNull(result);
        assertEquals(5, result.getTotalPayments()); // All 5 payments
        assertEquals(
                new BigDecimal("431.50"),
                result.getTotalAmount()); // 50.00 + 150.25 + 25.50 + 125.75 + 80.00
    }

    // Helper step for validating payments
    public static class ValidatePaymentStepBlocking extends ConfigurableStep
            implements StepOneToOneBlocking<TestPaymentEntity, TestPaymentEntity> {

        @Override
        public io.smallrye.mutiny.Uni<TestPaymentEntity> apply(TestPaymentEntity payment) {
            // Simulate some processing time
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Simple validation
            if (payment.getAmount() != null && payment.getAmount().compareTo(BigDecimal.ZERO) > 0) {
                payment.setStatus("VALIDATED");
            } else {
                payment.setStatus("REJECTED");
            }

            return io.smallrye.mutiny.Uni.createFrom().item(payment);
        }

        @Override
        public io.smallrye.mutiny.Uni<TestPaymentEntity> applyOneToOne(TestPaymentEntity input) {
            return apply(input);
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            super.initialiseWithConfig(config);
        }
    }

    // Helper step for aggregating payments (ManyToOne)
    public static class PaymentAggregationStep extends ConfigurableStep
            implements ManyToOne<TestPaymentEntity, PaymentSummary> {

        @Override
        public io.smallrye.mutiny.Uni<PaymentSummary> apply(
                io.smallrye.mutiny.Multi<TestPaymentEntity> input) {
            // Aggregate all payments into a single summary
            return input.collect()
                    .asList()
                    .onItem()
                    .transform(
                            list -> {
                                BigDecimal totalAmount = list.stream()
                                        .map(TestPaymentEntity::getAmount)
                                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                                return new PaymentSummary(list.size(), totalAmount);
                            });
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            super.initialiseWithConfig(config);
        }
    }

    // Helper class for test entities
    public static class TestPaymentEntity {
        private String recipient;
        private BigDecimal amount;
        private String status;

        public TestPaymentEntity(String recipient, BigDecimal amount) {
            this.recipient = recipient;
            this.amount = amount;
        }

        public String getRecipient() {
            return recipient;
        }

        public BigDecimal getAmount() {
            return amount;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

    // Helper class for test summary
    public static class PaymentSummary {
        private int totalPayments;
        private BigDecimal totalAmount;

        public PaymentSummary(int totalPayments, BigDecimal totalAmount) {
            this.totalPayments = totalPayments;
            this.totalAmount = totalAmount;
        }

        public int getTotalPayments() {
            return totalPayments;
        }

        public BigDecimal getTotalAmount() {
            return totalAmount;
        }
    }

    // Helper step for imperative aggregation
    public static class PaymentAggregationStepBlocking extends ConfigurableStep
            implements org.pipelineframework.step.blocking.StepManyToOneBlocking<TestPaymentEntity, PaymentSummary> {

        @Override
        public PaymentSummary applyBatchList(List<TestPaymentEntity> inputs) {
            // Aggregate payments in the batch
            BigDecimal totalAmount = inputs.stream()
                    .map(TestPaymentEntity::getAmount)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            return new PaymentSummary(inputs.size(), totalAmount);
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            super.initialiseWithConfig(config);
        }
    }
}
