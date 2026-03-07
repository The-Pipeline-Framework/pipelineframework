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
import org.pipelineframework.telemetry.BackpressureBufferMetrics;
import org.pipelineframework.telemetry.PipelineTelemetry;

/**
 * N -> 1 (reactive)
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface StepManyToOne<I, O> extends Configurable, ManyToOne<I, O>, ItemRejectable<I, O> {

    /** Logger for StepManyToOne operations. */
    Logger LOG = Logger.getLogger(StepManyToOne.class);

    /**
     * Apply the step to a stream of inputs and produce a single aggregated output.
     *
     * <p>The method applies the configured backpressure strategy to the input stream, applies the
     * step's reduction, enforces the step's retry policy on failures, and when configured delegates
     * failed processing to the item reject sink for recovery.
     *
     * @param input the stream of inputs to be processed
     * @return the single aggregated output produced by the step; if retries are exhausted the step
     *         either fails with the original error or, if recovery is enabled, yields the value
     *         produced by the item reject handling (which may be null)
     */
    @Override
    default Uni<O> apply(Multi<I> input) {
        
        // Apply overflow strategy to the input if needed
        Multi<I> backpressuredInput = input;
        final String strategy = backpressureStrategy();
        if ("buffer".equalsIgnoreCase(strategy)) {
            backpressuredInput =
                BackpressureBufferMetrics.buffer(backpressuredInput, this.getClass(), backpressureBufferCapacity());
        } else if ("drop".equalsIgnoreCase(strategy)) {
            backpressuredInput = backpressuredInput.onOverflow().drop();
        } else if (strategy == null || strategy.isBlank() || "default".equalsIgnoreCase(strategy)) {
            // default behavior - buffer with default capacity
            backpressuredInput = BackpressureBufferMetrics.buffer(backpressuredInput, this.getClass(), 128);
        } else {
            LOG.warnf("Unknown backpressure strategy '%s', defaulting to buffer(128)", strategy);
            backpressuredInput = BackpressureBufferMetrics.buffer(backpressuredInput, this.getClass(), 128);
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
            .invoke(t -> PipelineTelemetry.recordRetry(this.getClass()))
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

                    return rejectStream(finalInput, error);
                } else {
                    return Uni.createFrom().failure(error);
                }
            });
    }

    /**
 * Produce a single aggregated result by reducing the provided stream of inputs.
 *
 * Implementations should consume the provided stream and produce one final output value;
 * the input may already have had backpressure handling applied by the caller.
 *
 * @param input the stream of input items to be reduced
 * @return the aggregated output produced from the input stream
 */
    Uni<O> applyReduce(Multi<I> input);

}
