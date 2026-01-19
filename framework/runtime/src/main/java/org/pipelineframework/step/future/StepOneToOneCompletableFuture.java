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

package org.pipelineframework.step.future;

import java.util.concurrent.CompletableFuture;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.DeadLetterQueue;
import org.pipelineframework.step.functional.OneToOne;

/**
 * Imperative variant of StepOneToOne that works with CompletableFuture instead of Uni.
 * <p>
 * This interface is designed for developers who prefer imperative programming
 * and want to work with standard Java CompletableFuture instead of Mutiny Uni.
 * <p>
 * The PipelineRunner will automatically handle the conversion between reactive
 * and imperative representations.
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public interface StepOneToOneCompletableFuture<I, O> extends OneToOne<I, O>, Configurable, DeadLetterQueue<I, O> {
    /**
 * Process the given input and produce an output asynchronously.
 *
 * @param in the input item to process
 * @return a CompletableFuture completing with the produced output, or completing exceptionally if processing fails
 */
CompletableFuture<O> applyAsync(I in);

    /**
     * Adapts a Mutiny Uni input into the CompletableFuture-based processing defined by {@link #applyAsync(Object)},
     * applies retry/backoff/jitter policies, and performs optional dead-letter recovery and logging.
     *
     * <p>Behaviour summary:
     * - For each item emitted by {@code inputUni} the method delegates processing to {@link #applyAsync(Object)}.
     * - Failures are retried with exponential backoff and optional jitter up to {@link #retryLimit()} attempts;
     *   {@link NullPointerException} is excluded from retries.
     * - After exhausting retries an informational log entry is emitted identifying the step and retry limit.
     * - On success a debug log records the processed item; if recovery is enabled via {@link #recoverOnFailure()}
     *   a failed item is routed to the dead-letter queue via {@link #deadLetter(Uni, Throwable)} and a debug
     *   log records the failure, otherwise the failure is propagated.
     *
     * @param inputUni the Uni that emits input items to be processed
     * @return a Uni that emits processed output items or fails/recovers according to the configured retry and recovery policies
     */
    @Override
    default Uni<O> apply(Uni<I> inputUni) {
        final Logger LOG = Logger.getLogger(this.getClass());

        return inputUni
            .onItem().transformToUni(input -> {
                // call applyAsync on the plain input
                CompletableFuture<O> future = applyAsync(input);

                // wrap it into a Uni
                return Uni.createFrom().completionStage(future);
            })
            // retry / backoff / jitter
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
            })
            // debug logging
            .onItem().invoke(i -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debugf("Step %s processed item: %s", this.getClass().getSimpleName(), i);
                }
            })
            // recover with dead letter queue if needed
            .onFailure().recoverWithUni(err -> {
                if (recoverOnFailure()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debugf(
                                "Step %s: failed item=%s after %s retries: %s",
                                this.getClass().getSimpleName(), inputUni, retryLimit(), err
                        );
                    }
                    return deadLetter(inputUni, err);
                } else {
                    return Uni.createFrom().failure(err);
                }
            })
            .onTermination().invoke(() -> {
                // optional termination handler
            });
    }
}
