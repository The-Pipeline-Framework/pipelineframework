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

package org.pipelineframework.connector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jboss.logging.Logger;

/**
 * Runtime engine that maps, deduplicates, and forwards connector records.
 *
 * @param <I> source payload type
 * @param <O> target payload type
 */
public final class ConnectorRuntime<I, O> {
    private static final Logger LOG = Logger.getLogger(ConnectorRuntime.class);

    private final String connectorName;
    private final ConnectorSource<I> source;
    private final ConnectorTarget<O> target;
    private final Function<ConnectorRecord<I>, ConnectorRecord<O>> mapper;
    private final ConnectorPolicy policy;
    private final ConnectorIdempotencyTracker idempotencyTracker;
    private final Consumer<ConnectorRecord<I>> rejectionObserver;
    private final Consumer<ConnectorRecord<O>> duplicateObserver;
    private final Consumer<Throwable> failureObserver;

    public ConnectorRuntime(
        String connectorName,
        ConnectorSource<I> source,
        ConnectorTarget<O> target,
        Function<ConnectorRecord<I>, ConnectorRecord<O>> mapper,
        ConnectorPolicy policy,
        ConnectorIdempotencyTracker idempotencyTracker,
        Consumer<ConnectorRecord<I>> rejectionObserver,
        Consumer<ConnectorRecord<O>> duplicateObserver,
        Consumer<Throwable> failureObserver
    ) {
        this.connectorName = ConnectorSupport.normalizeOrDefault(connectorName, "connector");
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.target = Objects.requireNonNull(target, "target must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.policy = Objects.requireNonNull(policy, "policy must not be null");
        this.idempotencyTracker = policy.idempotencyPolicy() == ConnectorIdempotencyPolicy.DISABLED
            ? null
            : Objects.requireNonNull(idempotencyTracker, "idempotencyTracker must not be null when idempotency is enabled");
        this.rejectionObserver = rejectionObserver == null ? ignored -> {
        } : rejectionObserver;
        this.duplicateObserver = duplicateObserver == null ? ignored -> {
        } : duplicateObserver;
        this.failureObserver = failureObserver == null ? ignored -> {
        } : failureObserver;
    }

    public Cancellable start() {
        if (!policy.enabled()) {
            return () -> {
            };
        }
        Multi<ConnectorRecord<I>> backpressured = ConnectorSupport.applyBackpressure(
            source.stream(),
            policy.backpressurePolicy(),
            policy.backpressureBufferCapacity());
        Multi<ConnectorRecord<O>> forwarded = backpressured
            .onItem().transformToMulti(this::mapAndFilter)
            .concatenate();
        return target.forward(
            forwarded,
            this::markAccepted,
            this::handleFailure);
    }

    private Multi<ConnectorRecord<O>> mapAndFilter(ConnectorRecord<I> sourceRecord) {
        ConnectorRecord<O> mapped;
        try {
            mapped = mapper.apply(sourceRecord);
        } catch (RuntimeException e) {
            ConnectorMetrics.record(connectorName, "mapping_failure");
            failureObserver.accept(e);
            if (policy.failureMode() == ConnectorFailureMode.LOG_AND_CONTINUE) {
                LOG.warnf(e, "Connector %s mapping failure ignored due to LOG_AND_CONTINUE", connectorName);
                return Multi.createFrom().empty();
            }
            throw e;
        }
        if (mapped == null) {
            ConnectorMetrics.record(connectorName, "rejected");
            rejectionObserver.accept(sourceRecord);
            return Multi.createFrom().empty();
        }
        if (!tryAcquire(mapped)) {
            ConnectorMetrics.record(connectorName, "duplicate_suppressed");
            duplicateObserver.accept(mapped);
            return Multi.createFrom().empty();
        }
        ConnectorMetrics.record(connectorName, "forwarded");
        return Multi.createFrom().item(mapped);
    }

    private boolean tryAcquire(ConnectorRecord<O> mapped) {
        if (policy.idempotencyPolicy() == ConnectorIdempotencyPolicy.DISABLED) {
            return true;
        }
        String key = mapped.idempotencyKey();
        if (key == null || key.isBlank()) {
            LOG.debugf("Connector %s received mapped record without idempotency key while idempotency is enabled", connectorName);
            return true;
        }
        return idempotencyTracker.tryAcquire(key, policy.idempotencyPolicy());
    }

    private void markAccepted(ConnectorRecord<O> record) {
        if (policy.idempotencyPolicy() == ConnectorIdempotencyPolicy.DISABLED) {
            ConnectorMetrics.record(connectorName, "accepted");
            return;
        }
        String key = record == null ? null : record.idempotencyKey();
        if (key != null && !key.isBlank()) {
            idempotencyTracker.markAccepted(key, policy.idempotencyPolicy());
        }
        ConnectorMetrics.record(connectorName, "accepted");
    }

    private void handleFailure(Throwable failure) {
        if (idempotencyTracker != null) {
            idempotencyTracker.clearReservations();
        }
        ConnectorMetrics.record(connectorName, "delivery_failure");
        LOG.errorf(failure, "Connector %s delivery failed", connectorName);
        failureObserver.accept(failure);
    }
}
