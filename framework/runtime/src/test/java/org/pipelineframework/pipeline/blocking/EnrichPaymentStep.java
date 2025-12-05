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

package org.pipelineframework.pipeline.blocking;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.blocking.StepOneToOneBlocking;

/**
 * Another example of a blocking step that processes payment entities. This step demonstrates how to
 * enrich entities with additional data.
 */
public class EnrichPaymentStep extends ConfigurableStep
        implements StepOneToOneBlocking<TestPaymentEntity, TestPaymentEntity> {

    /**
     * Enriches a payment entity with additional data via a brief blocking simulation.
     *
     * <p>
     * If the payment's status equals "VALIDATED" the status is changed to "ENRICHED".
     *
     * @param payment the payment entity to process and potentially enrich
     * @return the input payment entity after enrichment
     */
    @Override
    public Uni<TestPaymentEntity> apply(TestPaymentEntity payment) {
        // This is a blocking operation that simulates enrichment logic

        // Simulate some processing time (blocking operation)
        try {
            Thread.sleep(50); // Blocking sleep to simulate work
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Enrich with additional data
        if ("VALIDATED".equals(payment.getStatus())) {
            // Simulate adding enriched data
            payment.setStatus("ENRICHED");
        }

        return Uni.createFrom().item(payment);
    }

    /**
     * Initialise the step with the supplied configuration.
     *
     * @param config the step configuration containing parameters used to initialise this step
     */
    @Override
    public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
        super.initialiseWithConfig(config);
    }

    public Uni<TestPaymentEntity> applyOneToOne(TestPaymentEntity input) {
        return apply(input);
    }
}
