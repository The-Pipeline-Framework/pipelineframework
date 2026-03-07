/*
 * Copyright (c) 2023-2026 Mariano Barcia
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
import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;
import org.pipelineframework.reject.ItemRejectRouter;

/**
 * Support for step-level reject-and-continue behaviour.
 *
 * @param <I> input type
 * @param <O> output type
 */
public interface ItemRejectable<I, O> {

    Logger LOG = Logger.getLogger(ItemRejectable.class);

    /**
     * Requests rejection of a single failed item to the configured reject sink.
     *
     * @param failedItem a Uni that yields the failed item (may be null)
     * @param cause the failure that triggered the reject
     * @return `null` output item to keep the pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectItem(Uni<I> failedItem, Throwable cause) {
        return rejectItem(failedItem, cause, 0, null);
    }

    /**
     * Send a single failed item to the configured reject sink and continue pipeline execution.
     *
     * If no reject sink is available, the method falls back to a local log-and-continue behavior.
     * If `cause` is `null`, a descriptive IllegalStateException is used instead.
     *
     * @param failedItem         the failed item wrapped in a Uni; may be `null`
     * @param cause              the failure cause; may be `null`
     * @param retriesObserved    the number of retries observed before the terminal failure; may be `null`
     * @param retryLimit         the configured retry limit for the item; may be `null`
     * @return                   `null` output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectItem(Uni<I> failedItem, Throwable cause, Integer retriesObserved, Integer retryLimit) {
        Uni<I> source = failedItem == null ? Uni.createFrom().nullItem() : failedItem.onFailure().recoverWithNull();
        Throwable normalizedCause = cause == null ? new IllegalStateException("Unknown reject failure") : cause;
        return source.onItem().transformToUni(item -> resolveRouter()
            .map(router -> router.<O>publishItemReject(this.getClass(), item, normalizedCause, retriesObserved, retryLimit))
            .orElseGet(() -> localFallbackReject(normalizedCause)));
    }

    /**
     * Rejects a stream of failed items to the configured reject sink and allows the pipeline to continue.
     *
     * @param input the stream of failed items to reject; may be null or empty
     * @param cause the failure cause; if null it is normalized to an IllegalStateException with message "Unknown reject failure"
     * @return the null output item used to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectStream(Multi<I> input, Throwable cause) {
        return rejectStream(input, cause, 0, null);
    }

    /**
     * Rejects a stream of failed items by publishing a sampled representation and total count to the configured reject sink.
     *
     * <p>If a CDI-provided ItemRejectRouter is available the rejection is published to it; otherwise a local log-and-continue fallback is used.</p>
     *
     * @param input the failed input stream; if `null` it is treated as an empty stream
     * @param cause the failure cause; if `null` it is replaced with an IllegalStateException describing an unknown reject failure
     * @param retriesObserved the number of retries observed before the terminal failure (may be `null`)
     * @param retryLimit the configured retry limit for the item/stream (may be `null`)
     * @return `null` (a null output item) to keep the pipeline flow alive when recovery is enabled
    default Uni<O> rejectStream(Multi<I> input, Throwable cause, Integer retriesObserved, Integer retryLimit) {
        Multi<I> source = input == null ? Multi.createFrom().empty() : input;
        Throwable normalizedCause = cause == null ? new IllegalStateException("Unknown reject failure") : cause;
        final int maxSampleSize = 5;

        return source
            .collect().in(
                () -> new StreamCollectionState<I>(new ArrayList<>(), 0L),
                (state, item) -> {
                    if (state.sample().size() < maxSampleSize) {
                        state.sample().add(item);
                    }
                    state.totalCount(state.totalCount() + 1L);
                })
            .onFailure().recoverWithItem(() -> new StreamCollectionState<>(new ArrayList<>(), 0L))
            .onItem().transformToUni(state -> resolveRouter()
                .map(router -> router.<O>publishStreamReject(
                    this.getClass(),
                    state.sample(),
                    state.totalCount(),
                    normalizedCause,
                    retriesObserved,
                    retryLimit))
                .orElseGet(() -> localFallbackReject(normalizedCause)));
    }

    /**
     * Resolve a CDI-managed ItemRejectRouter if one can be obtained.
     *
     * @return an Optional containing the resolved ItemRejectRouter, or an empty Optional if resolution fails or CDI is unavailable
     */
    private Optional<ItemRejectRouter> resolveRouter() {
        try {
            return Optional.of(CDI.current().select(ItemRejectRouter.class).get());
        } catch (Exception unavailable) {
            return Optional.empty();
        }
    }

    /**
     * Handle a rejection locally when the configured ItemRejectRouter is unavailable by logging the event
     * and returning a null output to allow the pipeline to continue.
     *
     * @param cause the reason for the reject (used for log output)
     * @return a `null` output item to keep the pipeline alive
     */
    private Uni<O> localFallbackReject(Throwable cause) {
        LOG.warnf("Item reject sink unavailable, falling back to local log-and-continue: %s", cause.toString());
        LOG.debug("Item reject fallback cause", cause);
        return Uni.createFrom().nullItem();
    }

    final class StreamCollectionState<T> {
        private final ArrayList<T> sample;
        private long totalCount;

        /**
         * Create a new state containing a sampled subset of stream items and the total number of items seen.
         *
         * @param sample     a list of sampled items from the stream (may be empty)
         * @param totalCount the total number of items observed in the stream
         */
        StreamCollectionState(ArrayList<T> sample, long totalCount) {
            this.sample = sample;
            this.totalCount = totalCount;
        }

        /**
         * Get the list of sampled items collected from the stream.
         *
         * @return the sampled items in insertion order
         */
        ArrayList<T> sample() {
            return sample;
        }

        /**
         * Returns the total number of items observed in the stream.
         *
         * @return the total count of items seen
         */
        long totalCount() {
            return totalCount;
        }

        /**
         * Sets the total number of items observed in the stream.
         *
         * @param totalCount the new total number of items observed
         */
        void totalCount(long totalCount) {
            this.totalCount = totalCount;
        }
    }
}
