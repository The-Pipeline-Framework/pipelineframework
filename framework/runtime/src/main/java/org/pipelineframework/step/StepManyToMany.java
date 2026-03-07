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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.smallrye.mutiny.Multi;
import org.jboss.logging.Logger;
import org.pipelineframework.step.functional.ManyToMany;
import org.pipelineframework.telemetry.BackpressureBufferMetrics;
import org.pipelineframework.telemetry.PipelineTelemetry;

/**
 * N -> N
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface StepManyToMany<I, O> extends Configurable, ManyToMany<I, O>, ItemRejectable<I, O> {
    /**
     * Apply the step to transform a stream of inputs to a stream of outputs.
     *
     * @param input the stream of input elements to process
     * @return a Multi that emits the transformed output elements
     */
    Multi<O> applyTransform(Multi<I> input);

	/**
     * Apply the step's transformation to the given input stream and attach backpressure handling, per-item debug logging and retry/backoff behaviour.
     *
     * <p>The resulting stream applies the configured overflow strategy, logs each emitted item at debug level, retries failures other than {@code NullPointerException}
     * using the configured backoff, jitter and retry limit, and logs an informational message if all retries are exhausted.</p>
     *
     * @param input the upstream Multi of input items to be transformed
     * @return a Multi emitting transformed output items with backpressure handling, per-item debug logging, and retry/backoff applied for failures (excluding {@code NullPointerException})
     */
    @Override
    default Multi<O> apply(Multi<I> input) {
        final Logger LOG = Logger.getLogger(this.getClass());
        final int maxSampleSize = 5;
        final ArrayList<I> sample = new ArrayList<>();
        final AtomicLong totalCount = new AtomicLong();
        Multi<I> trackedInput = input.onItem().invoke(item -> {
            totalCount.incrementAndGet();
            synchronized (sample) {
                if (sample.size() < maxSampleSize) {
                    sample.add(item);
                }
            }
        });

        // Apply the transformation
        Multi<O> output = applyTransform(trackedInput);

        // Apply overflow strategy
        String strategy = backpressureStrategy();
        if ("buffer".equalsIgnoreCase(strategy)) {
            output = BackpressureBufferMetrics.buffer(output, this.getClass(), backpressureBufferCapacity());
        } else if ("drop".equalsIgnoreCase(strategy)) {
            output = output.onOverflow().drop();
        } else if (strategy == null || strategy.isBlank() || "default".equalsIgnoreCase(strategy)) {
            output = BackpressureBufferMetrics.buffer(output, this.getClass(), 128);
        } else {
            LOG.warnf("Unknown backpressure strategy '%s', falling back to buffer(128)", strategy);
            // default behavior - buffer with default capacity
            output = BackpressureBufferMetrics.buffer(output, this.getClass(), 128);
        }

        return output.onItem().transform(item -> {
            if (LOG.isDebugEnabled()) {
                LOG.debugf(
                    "Step %s emitted item: %s",
                    this.getClass().getSimpleName(), item
                );
            }
            return item;
        })
        .onFailure(this::shouldRetry)
        .invoke(t -> PipelineTelemetry.recordRetry(this.getClass()))
        .onFailure(this::shouldRetry)
        .retry()
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
        })
        .onFailure().recoverWithMulti(error -> {
            if (recoverOnFailure()) {
                List<I> snapshot;
                synchronized (sample) {
                    snapshot = List.copyOf(sample);
                }
                return rejectStream(snapshot, totalCount.get(), error)
                    .onItem().transformToMulti(ignored -> Multi.createFrom().empty());
            }
            return Multi.createFrom().failure(error);
        });
    }
}
