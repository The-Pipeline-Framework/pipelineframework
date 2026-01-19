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

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.step.functional.OneToOne;

/**
 * Interface for one-to-one pipeline steps that transform a single input item to a single output
 * item asynchronously.
 *
 * <p>This interface represents a 1 -> 1 (async) transformation where each input item is processed
 * to produce exactly one output item. The processing is asynchronous, returning a Uni that emits
 * the transformed result.
 *
 * @param <I> the type of input item
 * @param <O> the type of output item
 */
public interface StepOneToOne<I, O> extends OneToOne<I, O>, Configurable, DeadLetterQueue<I, O> {

  /**
   * Apply the step to a single input and produce a single output.
   *
   * @param in the input element to process
   * @return a Uni that emits the transformed output element
   */
  Uni<O> applyOneToOne(I in);

  /**
   * Orchestrates the asynchronous one-to-one transformation of an emitted input item, performing input null checks, applying retries, and routing failures to a dead letter queue when configured.
   *
   * @param input the {@code Uni} that emits the input item to be transformed
   * @return the {@code Uni} that emits the transformed output item on success, or fails with the final error; if {@code recoverOnFailure()} is true a dead-lettered {@code Uni} may be returned instead of a propagated failure
   */
  @Override
  default Uni<O> apply(Uni<I> input) {
    final Logger LOG = Logger.getLogger(this.getClass());

    // Sanity check: input Uni itself is null
    if (input == null) {
      Throwable t = new NullPointerException("Input Uni is null");
      return recoverOnFailure()
          ? deadLetter(Uni.createFrom().failure(t), t)
          : Uni.createFrom().failure(t);
    }

    return input
        // Step 1: Null item becomes explicit failure
        .onItem()
        .ifNull()
        .failWith(() -> new NullPointerException("Input item is null"))

        // Step 2: Transform the item using gRPC call
        .onItem()
        .transformToUni(this::applyOneToOne)

        // Step 3: Apply retry policy for transient failures
        .onFailure(this::shouldRetry)
        .retry()
        .withBackOff(retryWait(), maxBackoff())
        .withJitter(jitter() ? 0.5 : 0.0)
        .atMost(retryLimit())

        // Step 4: Unified error handling after retries exhausted
        .onItemOrFailure()
        .transformToUni(
            (item, failure) -> {
              if (failure == null) {
                if (LOG.isDebugEnabled()) {
                  LOG.debugf("Step %s processed item: %s", this.getClass().getSimpleName(), item);
                }

                return Uni.createFrom().item(item);
              }

              // At this point, retries exhausted
              LOG.infof(
                  "Step %s failed after %s retries: %s",
                  this.getClass().getSimpleName(),
                  retryLimit(),
                  failure.toString());

              if (recoverOnFailure()) {
                return deadLetter(input, failure);
              } else {
                return Uni.createFrom().failure(failure);
              }
            });
  }
}
