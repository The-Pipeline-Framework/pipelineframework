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
import org.pipelineframework.step.functional.OneToMany;

/**
 * 1 -> N
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface StepOneToMany<I, O> extends OneToMany<I, O>, Configurable, DeadLetterQueue<I, O> {
    /**
     * Apply the step to a single input and produce multiple outputs.
     *
     * @param in the input element to process
     * @return a Multi that emits the transformed output elements
     */
    Multi<O> applyOneToMany(I in);

	/**
     * Converts a single asynchronous input into a stream of output items using this step's transformation and resilience policies.
     *
     * <p>The input item is passed to {@link #applyOneToMany(Object)} to produce an output Multi; the returned stream applies this step's configured
     * backpressure strategy and buffer capacity, emits each item while logging at debug level, retries failures (except {@link NullPointerException})
     * using the step's backoff, jitter and retry limit, and logs a final informational message if all retries are exhausted.</p>
     *
     * @param input the asynchronous input that produces the single item to process
     * @return a Multi that emits the transformed output items for the provided input, subject to backpressure and retry policies
     */
    @Override
    default Multi<O> apply(Uni<I> input) {
        final Logger LOG = Logger.getLogger(this.getClass());

        return input.onItem().transformToMulti(item -> {
            Multi<O> multi = applyOneToMany(item);

            // Apply overflow strategy
            if ("buffer".equalsIgnoreCase(backpressureStrategy())) {
                multi = multi.onOverflow().buffer(backpressureBufferCapacity());
            } else if ("drop".equalsIgnoreCase(backpressureStrategy())) {
                multi = multi.onOverflow().drop();
            } else {
                // default behavior - buffer with default capacity
                multi = multi.onOverflow().buffer(128); // default buffer size
            }

            return multi.onItem().transform(o -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debugf(
                        "Step %s emitted item: %s",
                        this.getClass().getSimpleName(), o
                    );
                }
                return o;
            });
        })
        .onFailure(this::shouldRetry).retry()
        .withBackOff(retryWait(), maxBackoff())
        .withJitter(jitter() ? 0.5 : 0.0)
        .atMost(retryLimit())
        .onFailure().invoke(t -> {
            LOG.infof(
                "Step %s completed all retries (%s attempts) with failure: %s",
                this.getClass().getSimpleName(),
                retryLimit(),
                t.getMessage()
            );
        });
    }
}
