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

import java.util.List;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.DeadLetterQueue;
import org.pipelineframework.step.functional.OneToMany;

/**
 * Imperative variant of StepOneToMany that works with Lists instead of Multi.
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
public interface StepOneToManyBlocking<I, O> extends Configurable, OneToMany<I, O>, DeadLetterQueue<I, O> {

    /** Logger for StepOneToManyBlocking operations. */
    Logger LOG = Logger.getLogger(StepOneToManyBlocking.class);

    /**
 * Produce a list of output items from a single input item.
 *
 * @param in the input item to be transformed
 * @return a List of output items produced from the input; an empty list if there are no outputs
 */
List<O> applyList(I in);


    /**
     * Convert a single-item reactive input into a stream of outputs produced by {@link #applyList(Object)}.
     *
     * For each item emitted by the provided Uni, this method emits each element of the List returned by
     * applyList for that item. On failures (except NullPointerException) it retries according to the
     * step's backoff, jitter and retry limit configuration; when all retries are exhausted it logs the final failure.
     *
     * @param inputUni a Uni that supplies the input item to be expanded into multiple outputs
     * @return a Multi that emits each output element produced from the input by {@link #applyList(Object)}
     */
    @Override
    default Multi<O> apply(Uni<I> inputUni) {

        return inputUni
            .onItem().transformToMulti(item -> {
                Multi<O> multi = Multi.createFrom().iterable(() -> {
                    try {
                        return applyList(item).iterator();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                return multi.onItem().invoke(o -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debugf("Blocking Step %s emitted item: %s", this.getClass().getSimpleName(), o);
                    }
                });
            })
            .onFailure(this::shouldRetry).retry()
            .withBackOff(retryWait(), maxBackoff())
            .withJitter(jitter() ? 0.5 : 0.0)
            .atMost(retryLimit())
            .onFailure().invoke(t -> {
                LOG.infof(
                    "Blocking Step %s completed all retries (%s attempts) with failure: %s",
                    this.getClass().getSimpleName(),
                    retryLimit(),
                    t.getMessage()
                );
            });
    }
}
