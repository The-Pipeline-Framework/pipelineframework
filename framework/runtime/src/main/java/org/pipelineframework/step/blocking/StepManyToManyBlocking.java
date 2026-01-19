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

import java.util.Collections;
import java.util.List;

import io.smallrye.mutiny.Multi;
import org.jboss.logging.Logger;
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.DeadLetterQueue;
import org.pipelineframework.step.functional.ManyToMany;

/**
 * Imperative variant of StepManyToMany that works with Lists instead of Multi.
 * <p>
 * This interface is designed for developers who prefer imperative programming
 * and want to work with standard Java collections instead of reactive streams.
 * <p>
 * The PipelineRunner will automatically handle the conversion between reactive
 * and imperative representations.
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface StepManyToManyBlocking<I, O> extends ManyToMany<I, O>, Configurable, DeadLetterQueue<I, O> {

    /** Logger for StepManyToManyBlocking operations. */
    Logger LOG = Logger.getLogger(StepManyToManyBlocking.class);

    /**
 * Process a batch of input items and produce the resulting output items.
 *
 * @param upstream the input items to process
 * @return the list of output items produced from the input batch
 */
List<O> applyStreamingList(List<I> upstream);

    /**
     * Produce a replacement output list when processing of an input batch fails.
     *
     * Called when a batch of input items cannot be processed; the default implementation
     * logs the error and indicates that the items are dropped to the dead-letter queue.
     *
     * @param upstream the original input items that failed processing
     * @param err the error that caused the failure
     * @return a list of output items to emit in place of the failed batch; the default implementation
     *         returns an empty list (items dropped)
     */
    default List<O> deadLetterList(List<I> upstream, Throwable err) {
        LOG.error("DLQ drop", err);
        return Collections.emptyList();
    }

    
    /**
     * Adapts a reactive input stream to the blocking many-to-many step and emits the resulting output items.
     *
     * <p>The implementation collects incoming items into a list, delegates processing of that list to
     * {@link #applyStreamingList(List)}, and emits the resulting items as a Multi. It applies the configured
     * backpressure strategy ("buffer" with capacity or "drop") to the input. If processing throws an exception,
     * and {@link #recoverOnFailure()} is true, a dead-letter list produced by {@link #deadLetterList(List, Throwable)}
     * is emitted instead; otherwise the failure is propagated. Processing failures are retried using the configured
     * retry parameters ({@link #retryWait()}, {@link #maxBackoff()}, {@link #jitter()}, {@link #retryLimit()}).
     * On final failure after all retries an informational log is recorded, and each emitted item is logged at debug level.
     *
     * @param input the source stream of input items to be collected and processed
     * @return a Multi that emits outputs produced from the collected input lists, or emits dead-letter items when configured; may fail after retries
     */
    @Override
    default Multi<O> apply(Multi<I> input) {

        // Apply overflow strategy to the input
        // default behavior - buffer with default capacity (no explicit overflow strategy needed)
        Multi<I> backpressuredInput = input;
        if ("buffer".equalsIgnoreCase(backpressureStrategy())) {
            backpressuredInput = backpressuredInput.onOverflow().buffer(backpressureBufferCapacity());
        } else if ("drop".equalsIgnoreCase(backpressureStrategy())) {
            backpressuredInput = backpressuredInput.onOverflow().drop();
        }

        return backpressuredInput
            .collect().asList()
            .onItem().transformToMulti(list -> {
                try {
                    List<O> result = applyStreamingList(list);
                    return Multi.createFrom().iterable(result);
                } catch (Exception e) {
                    if (recoverOnFailure()) {
                        List<O> dlqResult = deadLetterList(list, e);
                        return Multi.createFrom().iterable(dlqResult);
                    } else {
                        return Multi.createFrom().failure(e);
                    }
                }
            })
            .onFailure(this::shouldRetry).retry()
            .withBackOff(retryWait(), maxBackoff())
            .withJitter(jitter() ? 0.5 : 0.0)
            .atMost(retryLimit())
            .onFailure().invoke(t -> {
                LOG.infof(
                    "Blocking Step %s completed all retries (%d attempts) with failure: %s",
                    this.getClass().getSimpleName(),
                    retryLimit(),
                    t.getMessage()
                );
            })
            .onItem().invoke(o -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debugf("Blocking Step %s streamed item: %s",
                            this.getClass().getSimpleName(), o);
                }
            });
    }
}
