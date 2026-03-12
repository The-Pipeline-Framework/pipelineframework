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

    /**
     * Creates a ConnectorRuntime that wires a source to a target using the provided mapper and policy.
     *
     * The constructor normalizes the connector name, enforces non-null requirements for core components,
     * conditionally enables idempotency tracking based on the policy, and replaces any null observers with no-op consumers.
     *
     * @param connectorName       the connector name to normalize; defaults to "connector" if null or blank
     * @param source              the record source (must not be null)
     * @param target              the record target (must not be null)
     * @param mapper              function to map source records to target records (must not be null)
     * @param policy              connector policy controlling enablement, backpressure, idempotency and failure handling (must not be null)
     * @param idempotencyTracker  tracker used when idempotency is enabled; ignored and set to null when policy disables idempotency
     * @param rejectionObserver   invoked when a record is rejected by the mapper; if null a no-op consumer is used
     * @param duplicateObserver   invoked when a mapped record is identified as a duplicate; if null a no-op consumer is used
     * @param failureObserver     invoked on mapping or processing failures; if null a no-op consumer is used
     * @throws NullPointerException if source, target, mapper, or policy is null, or if idempotency is enabled and idempotencyTracker is null
     */
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

    /**
     * Starts processing records from the configured source, applying mapping, deduplication, backpressure, and forwarding to the target.
     *
     * @return a Cancellable that stops the runtime processing when invoked; if the connector policy is disabled, returns a no-op Cancellable
     */
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
        Cancellable active = target.forward(
            forwarded,
            this::markAccepted,
            this::handleFailure);
        return () -> {
            active.cancel();
            clearOutstandingReservations();
        };
    }

    /**
     * Map a source record to an output record and apply filtering rules for rejection, duplicates, and failures.
     *
     * @param sourceRecord the incoming source record to map and evaluate
     * @return a Multi that emits the mapped ConnectorRecord when it should be forwarded; an empty Multi when the record is rejected, identified as a duplicate, or skipped due to a mapping failure handled by policy
     * @throws RuntimeException if the mapper throws and the connector failure mode is not LOG_AND_CONTINUE
     */
    private Multi<ConnectorRecord<O>> mapAndFilter(ConnectorRecord<I> sourceRecord) {
        ConnectorRecord<O> mapped;
        try {
            mapped = mapper.apply(sourceRecord);
        } catch (RuntimeException e) {
            ConnectorMetrics.record(connectorName, "mapping_failure");
            invokeFailureObserver(e);
            if (policy.failureMode() == ConnectorFailureMode.LOG_AND_CONTINUE) {
                LOG.warnf(e, "Connector %s mapping failure ignored due to LOG_AND_CONTINUE", connectorName);
                return Multi.createFrom().empty();
            }
            throw e;
        }
        if (mapped == null) {
            ConnectorMetrics.record(connectorName, "rejected");
            invokeRejectionObserver(sourceRecord);
            return Multi.createFrom().empty();
        }
        if (!tryAcquire(mapped)) {
            ConnectorMetrics.record(connectorName, "duplicate_suppressed");
            invokeDuplicateObserver(mapped);
            return Multi.createFrom().empty();
        }
        ConnectorMetrics.record(connectorName, "forwarded");
        return Multi.createFrom().item(mapped);
    }

    /**
     * Determine whether the mapped record may be forwarded under the configured idempotency policy.
     *
     * @param mapped the mapped record whose idempotency key (if present) will be evaluated
     * @return `true` if the record should be forwarded (acquired); `false` if it is a duplicate and should be suppressed
     */
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

    /**
     * Marks a forwarded record as accepted and, if idempotency is enabled, informs the idempotency tracker.
     *
     * @param record the forwarded record; may be null. If non-null and the record's idempotency key is not blank,
     *               the key will be marked accepted according to the configured idempotency policy. An "accepted"
     *               metric is recorded in all cases.
     */
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

    /**
     * Handles a delivery failure by clearing any idempotency reservations, recording a delivery-failure metric,
     * logging the error, and notifying the configured failure observer.
     *
     * @param failure the delivery failure that occurred
     */
    private void handleFailure(Throwable failure) {
        if (idempotencyTracker != null) {
            idempotencyTracker.clearReservations();
        }
        ConnectorMetrics.record(connectorName, "delivery_failure");
        LOG.errorf(failure, "Connector %s delivery failed", connectorName);
        invokeFailureObserver(failure);
    }

    private void clearOutstandingReservations() {
        if (idempotencyTracker != null) {
            idempotencyTracker.clearReservations();
        }
    }

    private void invokeRejectionObserver(ConnectorRecord<I> sourceRecord) {
        try {
            rejectionObserver.accept(sourceRecord);
        } catch (Throwable e) {
            LOG.warnf(e, "Connector %s rejection observer failed", connectorName);
        }
    }

    private void invokeDuplicateObserver(ConnectorRecord<O> mapped) {
        try {
            duplicateObserver.accept(mapped);
        } catch (Throwable e) {
            LOG.warnf(e, "Connector %s duplicate observer failed", connectorName);
        }
    }

    private void invokeFailureObserver(Throwable failure) {
        try {
            failureObserver.accept(failure);
        } catch (Throwable e) {
            LOG.warnf(e, "Connector %s failure observer failed", connectorName);
        }
    }
}
