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

package org.pipelineframework.step.blocking;

import java.time.Duration;
import java.util.List;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.DeadLetterQueue;
import org.pipelineframework.step.functional.ManyToOne;

/**
 * N -> 1 (imperative)
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface StepManyToOneBlocking<I, O> extends Configurable, ManyToOne<I, O>, DeadLetterQueue<I, O> {

    /**
     * Apply the step to a batch of inputs, producing a single output.
     *
     * @param inputs The list of inputs to process
     * @return The single output
     */
    O applyBatchList(List<I> inputs);

    /**
     * The batch size for collecting inputs before processing.
     *
     * @return The batch size (default: 10)
     */
    default int batchSize() {
        return 10;
    }

    /**
     * The time window in milliseconds to wait before processing a batch,
     * even if the batch size hasn't been reached.
     *
     * @return The time window in milliseconds (default: 1000ms)
     */
    default Duration batchTimeout() {
        return Duration.ofMillis(1000);
    }

    /**
     * Deliver a failed input batch to a dead-letter mechanism and record the failure.
     *
     * Default implementation logs the error for the batch and yields a null result.
     *
     * @param inputs the batch of inputs that failed to process
     * @param error the error that occurred while processing the batch
     * @return a Uni emitting `null` cast to the output type `O`
     */
    default Uni<O> deadLetterBatchList(List<I> inputs, Throwable error) {
        Logger LOG = Logger.getLogger(this.getClass());
        LOG.errorf("DLQ drop for batch of %d items: %s", inputs.size(), error.getMessage());
        return Uni.createFrom().item((O) null);
    }

    /**
     * Process a stream of input items in configurable batches and emit the final batch's output.
     *
     * <p>The method applies the configured backpressure strategy ("buffer" or "drop"), groups items
     * into batches by {@link #batchSize()} or {@link #batchTimeout()}, and processes each batch by
     * calling {@link #applyBatchList(List)}. Batches are processed sequentially.
     * On failure, if {@link #recoverOnFailure()} is true the batch is delegated to
     * {@link #deadLetterBatchList(List, Throwable)}, otherwise the failure is propagated.
     * Retries are applied for failures except {@link NullPointerException} using the configured
     * backoff, jitter and retry limit settings.
     *
     * @param input the source stream of input items to be batched
     * @return the output produced for the last processed batch, or `null` if no batches were emitted
     */
    @Override
    default Uni<O> apply(Multi<I> input) {
        final Logger logger = Logger.getLogger(this.getClass());
        int batchSize = this.batchSize();
        Duration batchTimeout = this.batchTimeout();

        // Apply overflow strategy to the input
        // default behavior - buffer with default capacity (no explicit overflow strategy needed)
        Multi<I> backpressuredInput = input;
        if ("buffer".equalsIgnoreCase(backpressureStrategy())) {
            backpressuredInput = backpressuredInput.onOverflow().buffer(backpressureBufferCapacity());
        } else if ("drop".equalsIgnoreCase(backpressureStrategy())) {
            backpressuredInput = backpressuredInput.onOverflow().drop();
        }

        Multi<List<I>> batches = backpressuredInput
            .group().intoLists().of(batchSize, batchTimeout);

        return batches
            .onItem().transformToUniAndConcatenate(list -> processBatch(list, logger))
            .collect().last();
    }

    /**
     * Process a single batch of inputs with proper error handling and retry logic.
     *
     * @param list The batch of inputs to process
     * @param logger The logger to use for logging
     * @return A Uni that emits the processed output or handles errors appropriately
     */
    private Uni<O> processBatch(List<I> list, Logger logger) {
        return Uni.createFrom().item(list)
            .onItem().transformToUni(batch -> {
                try {
                    O result = applyBatchList(batch);

                    if (logger.isDebugEnabled()) {
                        logger.debugf(
                            "Blocking Step %s processed batch of %d items into single output: %s",
                            this.getClass().getSimpleName(), batch.size(), result
                        );
                    }

                    return Uni.createFrom().item(result);
                } catch (Exception e) {
                    if (recoverOnFailure()) {
                        if (logger.isDebugEnabled()) {
                            logger.debugf(
                                "Blocking Step %s: failed batch: %s",
                                this.getClass().getSimpleName(), e.getMessage()
                            );
                        }
                        return deadLetterBatchList(list, e);
                    } else {
                        return Uni.createFrom().failure(e);
                    }
                }
            })
            .onFailure(this::shouldRetry).retry()
            .withBackOff(retryWait(), maxBackoff())
            .withJitter(jitter() ? 0.5 : 0.0)
            .atMost(retryLimit());
    }
}
