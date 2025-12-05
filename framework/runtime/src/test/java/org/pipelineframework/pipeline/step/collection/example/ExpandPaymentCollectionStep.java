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

package org.pipelineframework.pipeline.step.collection.example;

import java.util.ArrayList;
import java.util.List;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.blocking.StepOneToManyBlocking;

/**
 * Example of a collection-based 1:N step that expands a single payment request into multiple
 * transactions.
 *
 * <p>
 * This step demonstrates how to use the StepOneToManyBlocking interface for operations that need
 * to expand a single input into multiple outputs using standard Java collections.
 */
public class ExpandPaymentCollectionStep extends ConfigurableStep
        implements StepOneToManyBlocking<String, String> {

    @Override
    public List<String> applyList(String paymentRequest) {
        // This is a blocking operation that simulates expanding one payment into multiple
        // transactions
        // In a real application, this might call external services or perform complex calculations

        // Simulate some processing time (blocking operation)
        try {
            Thread.sleep(100); // Blocking sleep to simulate work
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Expand the single payment request into multiple transactions
        List<String> transactions = new ArrayList<>();
        transactions.add("TXN-001-" + paymentRequest);
        transactions.add("TXN-002-" + paymentRequest);
        transactions.add("TXN-003-" + paymentRequest);

        return transactions;
    }
}
