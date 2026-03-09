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

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.CreationException;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;
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
     * @param failedItem failed item value
     * @param cause failure cause
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectItem(I failedItem, Throwable cause) {
        return rejectItem(failedItem, cause, 0, null);
    }

    /**
     * Rejects one failed item through the configured reject sink.
     *
     * @param failedItem failed item value
     * @param cause failure cause
     * @param retriesObserved retries observed before terminal failure
     * @param retryLimit configured retry limit
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectItem(I failedItem, Throwable cause, Integer retriesObserved, Integer retryLimit) {
        Throwable normalizedCause = cause == null ? new IllegalStateException("Unknown reject failure") : cause;
        // Capture context before reactive thread hops; thread-local holders may be empty later in async chains.
        TransportDispatchMetadata transport = TransportDispatchMetadataHolder.get();
        PipelineContext context = PipelineContextHolder.get();
        return resolveRouter()
            .map(router -> router.<O>publishItemReject(
                this.getClass(),
                failedItem,
                normalizedCause,
                retriesObserved,
                retryLimit,
                transport,
                context))
            .orElseGet(() -> localFallbackReject(normalizedCause));
    }

    /**
     * Rejects a failed stream through the configured reject sink.
     *
     * @param sampleItems sample items from the failed stream
     * @param totalItemCount total items seen in the failed stream
     * @param cause failure cause
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectStream(List<I> sampleItems, long totalItemCount, Throwable cause) {
        return rejectStream(sampleItems, totalItemCount, cause, 0, null);
    }

    /**
     * Rejects a failed stream through the configured reject sink.
     *
     * @param sampleItems sample items from the failed stream
     * @param totalItemCount total items seen in the failed stream
     * @param cause failure cause
     * @param retriesObserved retries observed before terminal failure
     * @param retryLimit configured retry limit
     * @return null output item to keep pipeline flow alive when recovery is enabled
     */
    default Uni<O> rejectStream(
        List<I> sampleItems,
        long totalItemCount,
        Throwable cause,
        Integer retriesObserved,
        Integer retryLimit
    ) {
        List<I> safeSample = sampleItems == null ? List.of() : List.copyOf(sampleItems);
        Throwable normalizedCause = cause == null ? new IllegalStateException("Unknown reject failure") : cause;
        // Capture context before reactive thread hops; thread-local holders may be empty later in async chains.
        TransportDispatchMetadata transport = TransportDispatchMetadataHolder.get();
        PipelineContext context = PipelineContextHolder.get();
        return resolveRouter()
            .map(router -> router.<O>publishStreamReject(
                this.getClass(),
                safeSample,
                Math.max(0L, totalItemCount),
                normalizedCause,
                retriesObserved,
                retryLimit,
                transport,
                context))
            .orElseGet(() -> localFallbackReject(normalizedCause));
    }

    private Optional<ItemRejectRouter> resolveRouter() {
        CDI<Object> cdi;
        try {
            cdi = CDI.current();
        } catch (IllegalStateException unavailable) {
            return Optional.empty();
        }

        Instance<ItemRejectRouter> instance;
        try {
            instance = cdi.select(ItemRejectRouter.class);
        } catch (RuntimeException selectionFailure) {
            LOG.warnf(selectionFailure, "Failed selecting ItemRejectRouter from CDI.");
            return Optional.empty();
        }
        if (instance.isUnsatisfied()) {
            return Optional.empty();
        }

        try {
            return Optional.of(instance.get());
        } catch (ContextNotActiveException | CreationException creationFailure) {
            LOG.warnf(creationFailure, "Item reject sink unavailable because ItemRejectRouter is not active.");
            return Optional.empty();
        } catch (RuntimeException creationFailure) {
            LOG.warnf(creationFailure, "Failed to create ItemRejectRouter for item reject handling.");
            return Optional.empty();
        }
    }

    private Uni<O> localFallbackReject(Throwable cause) {
        LOG.warnf("Item reject sink unavailable, falling back to local log-and-continue: %s", cause.toString());
        LOG.debug("Item reject fallback cause", cause);
        return Uni.createFrom().nullItem();
    }
}
