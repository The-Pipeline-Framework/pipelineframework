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

package org.pipelineframework.pipeline.step.future.example;

import java.util.concurrent.CompletableFuture;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;

/**
 * Example of a Future-based step that processes a payment asynchronously.
 *
 * <p>
 * This step demonstrates how to use the StepOneToOneCompletableFuture interface for operations
 * that need to process inputs asynchronously using standard Java CompletableFuture.
 */
public class ProcessPaymentFutureStep extends ConfigurableStep
        implements StepOneToOneCompletableFuture<String, String> {

    @Override
    public CompletableFuture<String> applyAsync(String paymentRequest) {
        // This returns a CompletableFuture that simulates async processing
        return CompletableFuture.supplyAsync(
                () -> {
                    // Simulate some async processing time
                    try {
                        Thread.sleep(200); // Simulate async work
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    // Process the payment request
                    return "Processed: " + paymentRequest;
                });
    }
}
