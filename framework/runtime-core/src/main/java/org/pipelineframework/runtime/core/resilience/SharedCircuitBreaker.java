package org.pipelineframework.runtime.core.resilience;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounded-staleness shared dependency circuit admission.
 *
 * <p>CLOSED admissions use a fresh local snapshot. OPEN visibility is therefore bounded by the
 * configured staleness interval; transitions and HALF_OPEN leases always use the store authority.
 * Store failures never degrade a shared policy to process-local protection.</p>
 */
public final class SharedCircuitBreaker implements CircuitBreaker {
    private final SharedCircuitStateStore store;
    private final Clock clock;
    private final java.time.Duration maxStateStaleness;
    private final java.time.Duration backendRetryDelay;
    private final String owner;
    private final CircuitBreakerListener listener;
    private final ConcurrentMap<CircuitIdentity, CachedSnapshot> cache = new ConcurrentHashMap<>();
    private final ConcurrentMap<CircuitIdentity, FailureFlush> failureFlushes = new ConcurrentHashMap<>();
    private final Object failureFlushLock = new Object();

    public SharedCircuitBreaker(
        SharedCircuitStateStore store,
        Clock clock,
        java.time.Duration maxStateStaleness,
        java.time.Duration backendRetryDelay,
        CircuitBreakerListener listener
    ) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.maxStateStaleness = positive(maxStateStaleness, "maxStateStaleness");
        this.backendRetryDelay = positive(backendRetryDelay, "backendRetryDelay");
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        this.owner = UUID.randomUUID().toString();
    }

    @Override
    public CompletionStage<CircuitDecision> acquire(CircuitIdentity identity, CircuitPolicy policy) {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(policy, "policy must not be null");
        if (policy.requiredScope() != CircuitScope.SHARED_DEPENDENCY) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "SharedCircuitBreaker only guarantees " + CircuitScope.SHARED_DEPENDENCY));
        }
        Instant now = clock.instant();
        CachedSnapshot cached = cache.get(identity);
        if (cached != null && cached.isFresh(now, maxStateStaleness)) {
            return decisionForFreshSnapshot(identity, policy, cached.snapshot(), now);
        }
        return store.read(identity, policy)
            .thenCompose(snapshot -> {
                cache(identity, snapshot, now);
                return decisionForFreshSnapshot(identity, policy, snapshot, now);
            })
            .exceptionally(error -> unavailable(identity, now));
    }

    private CompletionStage<CircuitDecision> decisionForFreshSnapshot(
        CircuitIdentity identity,
        CircuitPolicy policy,
        SharedCircuitSnapshot snapshot,
        Instant now
    ) {
        return switch (snapshot.status()) {
            case CLOSED -> CompletableFuture.completedFuture(permitted(identity, policy));
            case OPEN -> now.isBefore(snapshot.openUntil())
                ? CompletableFuture.completedFuture(rejected(identity, policy, snapshot.openUntil()))
                : acquireProbe(identity, policy, now);
            case HALF_OPEN -> acquireProbe(identity, policy, now);
        };
    }

    private CompletionStage<CircuitDecision> acquireProbe(
        CircuitIdentity identity,
        CircuitPolicy policy,
        Instant now
    ) {
        return store.acquireHalfOpenProbe(identity, policy, owner, now)
            .thenApply(decision -> {
                cache(identity, decision.snapshot(), now);
                if (decision.permitted()) {
                    return new CircuitDecision.Permitted(new SharedPermit(
                        identity, policy, decision.probe().orElseThrow()));
                }
                if (decision.snapshot().status() == SharedCircuitStatus.CLOSED) {
                    return permitted(identity, policy);
                }
                return rejected(identity, policy, decision.notBefore());
            })
            .exceptionally(error -> unavailable(identity, now));
    }

    private CircuitDecision permitted(CircuitIdentity identity, CircuitPolicy policy) {
        return new CircuitDecision.Permitted(new ClosedPermit(identity, policy));
    }

    private static CircuitDecision rejected(CircuitIdentity identity, CircuitPolicy policy, Instant notBefore) {
        return new CircuitDecision.Rejected(new CircuitOpen(identity, policy.requiredScope(), notBefore));
    }

    private CircuitDecision unavailable(CircuitIdentity identity, Instant now) {
        return new CircuitDecision.Unavailable(new CircuitProtectionUnavailable(
            identity, CircuitScope.SHARED_DEPENDENCY, now.plus(backendRetryDelay)));
    }

    private CompletionStage<Void> recordHealthFailure(CircuitIdentity identity, CircuitPolicy policy) {
        synchronized (failureFlushLock) {
            FailureFlush flush = failureFlushes.get(identity);
            if (flush == null) {
                flush = new FailureFlush(identity, policy);
                failureFlushes.put(identity, flush);
            }
            return flush.record();
        }
    }

    private void cache(CircuitIdentity identity, SharedCircuitSnapshot snapshot, Instant observedAt) {
        CachedSnapshot previous = cache.put(identity, new CachedSnapshot(snapshot, observedAt));
        if (previous != null) {
            transition(previous.snapshot().status(), snapshot.status()).ifPresent(change -> notifyTransition(identity, change));
        }
    }

    private void notifyTransition(CircuitIdentity identity, CircuitStateTransition transition) {
        try {
            listener.onTransition(identity, CircuitScope.SHARED_DEPENDENCY, transition);
        } catch (RuntimeException ignored) {
            // Telemetry is best effort and cannot affect admission.
        }
    }

    private static Optional<CircuitStateTransition> transition(
        SharedCircuitStatus previous,
        SharedCircuitStatus current
    ) {
        if (previous == current) {
            return Optional.empty();
        }
        return switch (previous) {
            case CLOSED -> current == SharedCircuitStatus.OPEN
                ? Optional.of(CircuitStateTransition.CLOSED_TO_OPEN) : Optional.empty();
            case OPEN -> current == SharedCircuitStatus.HALF_OPEN
                ? Optional.of(CircuitStateTransition.OPEN_TO_HALF_OPEN) : Optional.empty();
            case HALF_OPEN -> switch (current) {
                case CLOSED -> Optional.of(CircuitStateTransition.HALF_OPEN_TO_CLOSED);
                case OPEN -> Optional.of(CircuitStateTransition.HALF_OPEN_TO_OPEN);
                case HALF_OPEN -> Optional.empty();
            };
        };
    }

    private final class ClosedPermit implements CircuitPermit {
        private final CircuitIdentity identity;
        private final CircuitPolicy policy;
        private final AtomicBoolean completed = new AtomicBoolean();

        private ClosedPermit(CircuitIdentity identity, CircuitPolicy policy) {
            this.identity = identity;
            this.policy = policy;
        }

        @Override
        public CompletionStage<Void> complete(CircuitOutcome outcome) {
            Objects.requireNonNull(outcome, "outcome must not be null");
            if (completed.compareAndSet(false, true) && outcome == CircuitOutcome.HEALTH_FAILURE) {
                return recordHealthFailure(identity, policy);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private final class SharedPermit implements CircuitPermit {
        private final CircuitIdentity identity;
        private final CircuitPolicy policy;
        private final SharedCircuitProbe probe;
        private final AtomicBoolean completed = new AtomicBoolean();

        private SharedPermit(CircuitIdentity identity, CircuitPolicy policy, SharedCircuitProbe probe) {
            this.identity = identity;
            this.policy = policy;
            this.probe = probe;
        }

        @Override
        public CompletionStage<Void> complete(CircuitOutcome outcome) {
            Objects.requireNonNull(outcome, "outcome must not be null");
            if (!completed.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            }
            Instant now = clock.instant();
            return store.completeProbe(identity, policy, probe, outcome, now)
                .thenAccept(ignored -> cache.remove(identity));
        }
    }

    /** Coalesces concurrently observed closed-state failures; the first one flushes immediately. */
    private final class FailureFlush {
        private final CircuitIdentity identity;
        private final CircuitPolicy policy;
        private int pending;
        private boolean flushing;
        private CompletableFuture<Void> active = new CompletableFuture<>();

        private FailureFlush(CircuitIdentity identity, CircuitPolicy policy) {
            this.identity = identity;
            this.policy = policy;
        }

        private CompletionStage<Void> record() {
            FailureBatch batch = null;
            CompletableFuture<Void> result;
            synchronized (this) {
                pending++;
                result = active;
                if (!flushing) {
                    flushing = true;
                    batch = claimPending();
                }
            }
            if (batch != null) {
                flush(batch);
            }
            return result;
        }

        private FailureBatch claimPending() {
            FailureBatch batch = new FailureBatch(pending, active);
            pending = 0;
            active = new CompletableFuture<>();
            return batch;
        }

        private void flush(FailureBatch batch) {
            store.recordHealthFailures(identity, policy, batch.failures(), clock.instant())
                .whenComplete((snapshot, error) -> {
                    if (error == null) {
                        cache(identity, snapshot, clock.instant());
                        batch.completion().complete(null);
                    } else {
                        batch.completion().completeExceptionally(error);
                    }
                    FailureBatch next = null;
                    synchronized (this) {
                        if (pending > 0) {
                            next = claimPending();
                        } else {
                            flushing = false;
                        }
                    }
                    if (next != null) {
                        flush(next);
                    } else {
                        removeWhenIdle();
                    }
                });
        }

        private void removeWhenIdle() {
            synchronized (failureFlushLock) {
                synchronized (this) {
                    if (!flushing && pending == 0) {
                        failureFlushes.remove(identity, this);
                    }
                }
            }
        }

        private record FailureBatch(int failures, CompletableFuture<Void> completion) {
        }
    }

    private record CachedSnapshot(SharedCircuitSnapshot snapshot, Instant observedAt) {
        private boolean isFresh(Instant now, java.time.Duration maxStaleness) {
            return !observedAt.plus(maxStaleness).isBefore(now);
        }
    }

    private static java.time.Duration positive(java.time.Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
        return value;
    }
}
