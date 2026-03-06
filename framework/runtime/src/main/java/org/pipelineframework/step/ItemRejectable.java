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
     * Rejects one failed item through the configured reject sink.
     *
     * @param failedItem failed item wrapper
     * @param cause failure cause
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectItem(Uni<I> failedItem, Throwable cause) {
        return rejectItem(failedItem, cause, 0, null);
    }

    /**
     * Rejects one failed item through the configured reject sink.
     *
     * @param failedItem failed item wrapper
     * @param cause failure cause
     * @param retriesObserved retries observed before terminal failure
     * @param retryLimit configured retry limit
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectItem(Uni<I> failedItem, Throwable cause, Integer retriesObserved, Integer retryLimit) {
        Uni<I> source = failedItem == null ? Uni.createFrom().nullItem() : failedItem.onFailure().recoverWithNull();
        Throwable normalizedCause = cause == null ? new IllegalStateException("Unknown reject failure") : cause;
        return source.onItem().transformToUni(item -> resolveRouter()
            .map(router -> router.<O>publishItemReject(this.getClass(), item, normalizedCause, retriesObserved, retryLimit))
            .orElseGet(() -> localFallbackReject(normalizedCause)));
    }

    /**
     * Rejects a failed stream through the configured reject sink.
     *
     * @param input failed input stream
     * @param cause failure cause
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectStream(Multi<I> input, Throwable cause) {
        return rejectStream(input, cause, 0, null);
    }

    /**
     * Rejects a failed stream through the configured reject sink.
     *
     * @param input failed input stream
     * @param cause failure cause
     * @param retriesObserved retries observed before terminal failure
     * @param retryLimit configured retry limit
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
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

    private Optional<ItemRejectRouter> resolveRouter() {
        try {
            return Optional.of(CDI.current().select(ItemRejectRouter.class).get());
        } catch (Exception unavailable) {
            return Optional.empty();
        }
    }

    private Uni<O> localFallbackReject(Throwable cause) {
        LOG.warnf("Item reject sink unavailable, falling back to local log-and-continue: %s", cause.toString());
        LOG.debug("Item reject fallback cause", cause);
        return Uni.createFrom().nullItem();
    }

    final class StreamCollectionState<T> {
        private final ArrayList<T> sample;
        private long totalCount;

        StreamCollectionState(ArrayList<T> sample, long totalCount) {
            this.sample = sample;
            this.totalCount = totalCount;
        }

        ArrayList<T> sample() {
            return sample;
        }

        long totalCount() {
            return totalCount;
        }

        void totalCount(long totalCount) {
            this.totalCount = totalCount;
        }
    }
}
