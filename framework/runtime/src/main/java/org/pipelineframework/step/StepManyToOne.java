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

package org.pipelineframework.step;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.step.functional.ManyToOne;

/**
 * N -> 1 (reactive)
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface StepManyToOne<I, O> extends Configurable, ManyToOne<I, O>, DeadLetterQueue<I, O> {

    /** Logger for StepManyToOne operations. */
    Logger LOG = Logger.getLogger(StepManyToOne.class);

    /**
     * Apply the step to a stream of inputs and produce a single aggregated output.
     *
     * <p>The method applies the configured backpressure strategy to the provided input stream,
     * applies retry semantics on failures (excluding NullPointerException), and — if configured —
     * recovers failed processing by delegating the collected stream to the dead-letter handler.
     *
     * @param input the stream of inputs to be processed
     * @return a Uni that emits the step's single output; if retries are exhausted the Uni will
     *         either fail with the original error or, if recovery is enabled, emit the value
     *         produced by the dead-letter handling (which may be null)
     */
    @Override
    default Uni<O> apply(Multi<I> input) {
        
        // Apply overflow strategy to the input if needed
        Multi<I> backpressuredInput = input;
        final String strategy = backpressureStrategy();
        if ("buffer".equalsIgnoreCase(strategy)) {
            backpressuredInput = backpressuredInput.onOverflow().buffer(backpressureBufferCapacity());
        } else if ("drop".equalsIgnoreCase(strategy)) {
            backpressuredInput = backpressuredInput.onOverflow().drop();
        } else if (strategy == null || strategy.isBlank() || "default".equalsIgnoreCase(strategy)) {
            // default behavior - buffer with default capacity
            backpressuredInput = backpressuredInput.onOverflow().buffer(128); // default buffer size
        } else {
            LOG.warnf("Unknown backpressure strategy '%s', defaulting to buffer(128)", strategy);
            backpressuredInput = backpressuredInput.onOverflow().buffer(128); // default buffer size
        }

        final Multi<I> finalInput = backpressuredInput;

        return applyReduce(finalInput)
            .onItem().invoke(resultValue -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debugf("Reactive Step %s processed stream into output: %s",
                        this.getClass().getSimpleName(), resultValue);
                }
            })
            .onFailure(this::shouldRetry)
            .retry()
            .withBackOff(retryWait(), maxBackoff())
            .withJitter(jitter() ? 0.5 : 0.0)
            .atMost(retryLimit())
            .onFailure().recoverWithUni(error -> {
                if (recoverOnFailure()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debugf("Reactive Step %s: failed to process stream: %s",
                            this.getClass().getSimpleName(), error.getMessage());
                    }

                    return deadLetterStream(finalInput, error);
                } else {
                    return Uni.createFrom().failure(error);
                }
            });
    }

    /**
     * Apply the reduction operation to a stream of inputs, producing a single output.
     * This method would typically be implemented by gRPC client adapters.
     *
     * @param input The stream of inputs as a Multi
     * @return The single output as a Uni
     */
    Uni<O> applyReduce(Multi<I> input);

    /**
     * Handle a failed stream by sending it to a dead letter queue or similar mechanism reactively.
     *
     * @param input The input stream that failed to process
     * @param error The error that occurred
     * @return The result of dead letter handling as a Uni (can be null)
     */
    default Uni<O> deadLetterStream(Multi<I> input, Throwable error) {
        // Perform a single pass to collect a sample and count the total items
        final int maxSampleSize = 5; // Only keep a few sample items to avoid memory issues

        // Cache the items and count to handle both successful and failed streams
        // If the input stream fails, we'll still return a successful result with zero count
        return input
            .collect().in(
                // Supplier: Initialize the collection state with empty list and zero count
                () -> new java.util.AbstractMap.SimpleEntry<>(new java.util.ArrayList<I>(), 0L),
                // Accumulator: Add item to sample list if under maxSampleSize, increment count
                (state, item) -> {
                    java.util.List<I> sampleList = state.getKey();
                    Long count = state.getValue();

                    if (sampleList.size() < maxSampleSize) {
                        sampleList.add(item);
                    }
                    state.setValue(count + 1);
                }
            )
            // If collecting the stream fails, recover with an empty state (no items, count 0)
            .onFailure().recoverWithItem(throwable -> {
                LOG.debug("Stream failed during dead letter collection, returning empty state", throwable);
                return new java.util.AbstractMap.SimpleEntry<>(new java.util.ArrayList<>(), 0L);
            })
            .onItem().transformToUni(state -> {
                java.util.List<I> sampleList = state.getKey();
                Long count = state.getValue();

                String sampleInfo;
                if (!sampleList.isEmpty()) {
                    sampleInfo = String.format("first %d of %d items",
                        Math.min(sampleList.size(), maxSampleSize), count);
                } else {
                    sampleInfo = String.format("%d items", count);
                }
                LOG.errorf("DLQ drop for stream with %s: %s", sampleInfo, error.getMessage());
                return Uni.createFrom().nullItem();
            });
    }
}
