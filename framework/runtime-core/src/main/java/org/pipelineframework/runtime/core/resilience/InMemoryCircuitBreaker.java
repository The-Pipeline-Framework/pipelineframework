package org.pipelineframework.runtime.core.resilience;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-scoped circuit breaker intended for non-durable invocations.
 *
 * <p>Every state transition advances a generation. A permit can affect state only while its
 * captured generation remains current, so a late half-open result cannot overwrite a newer
 * reopened or closed generation.</p>
 */
public final class InMemoryCircuitBreaker implements CircuitBreaker {
    private final Clock clock;
    private final CircuitBreakerListener listener;
    private final ConcurrentMap<CircuitIdentity, CircuitState> circuits = new ConcurrentHashMap<>();

    public InMemoryCircuitBreaker() {
        this(Clock.systemUTC(), CircuitBreakerListener.NOOP);
    }

    public InMemoryCircuitBreaker(Clock clock) {
        this(clock, CircuitBreakerListener.NOOP);
    }

    public InMemoryCircuitBreaker(Clock clock, CircuitBreakerListener listener) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
    }

    @Override
    public CompletionStage<CircuitDecision> acquire(CircuitIdentity identity, CircuitPolicy policy) {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(policy, "policy must not be null");
        if (policy.requiredScope() != CircuitScope.LOCAL_PROCESS) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "InMemoryCircuitBreaker only guarantees " + CircuitScope.LOCAL_PROCESS));
        }

        CircuitStateTransition transition = null;
        CircuitDecision decision;
        CircuitState state = circuits.computeIfAbsent(identity, ignored -> new CircuitState());
        synchronized (state) {
            Instant now = clock.instant();
            state.discardFailuresBefore(now.minus(policy.failureWindow()));

            if (state.status == Status.OPEN) {
                if (now.isBefore(state.openUntil)) {
                    return CompletableFuture.completedFuture(rejected(identity, policy, state.openUntil));
                }
                state.enterHalfOpen();
                transition = CircuitStateTransition.OPEN_TO_HALF_OPEN;
            }
            if (state.status == Status.HALF_OPEN) {
                if (state.halfOpenPermitsInFlight >= policy.halfOpenMaxPermits()) {
                    Instant notBefore = state.nextHalfOpenRejection.isAfter(now)
                        ? state.nextHalfOpenRejection
                        : now.plus(policy.halfOpenRetryDelay());
                    state.nextHalfOpenRejection = notBefore.plus(policy.halfOpenRetryDelay());
                    return CompletableFuture.completedFuture(rejected(identity, policy, notBefore));
                }
                state.halfOpenPermitsInFlight++;
                decision = permitted(identity, state, policy, true);
            } else {
                decision = permitted(identity, state, policy, false);
            }
        }
        notifyTransition(identity, policy, transition);
        return CompletableFuture.completedFuture(decision);
    }

    private CircuitDecision permitted(
        CircuitIdentity identity,
        CircuitState state,
        CircuitPolicy policy,
        boolean halfOpen
    ) {
        return new CircuitDecision.Permitted(new Permit(identity, state, policy, halfOpen, state.generation));
    }

    private CircuitDecision rejected(CircuitIdentity identity, CircuitPolicy policy, Instant notBefore) {
        return new CircuitDecision.Rejected(new CircuitOpen(identity, policy.requiredScope(), notBefore));
    }

    private final class Permit implements CircuitPermit {
        private final CircuitIdentity identity;
        private final CircuitState state;
        private final CircuitPolicy policy;
        private final boolean halfOpen;
        private final long generation;
        private final AtomicBoolean completed = new AtomicBoolean();

        private Permit(
            CircuitIdentity identity,
            CircuitState state,
            CircuitPolicy policy,
            boolean halfOpen,
            long generation
        ) {
            this.identity = identity;
            this.state = state;
            this.policy = policy;
            this.halfOpen = halfOpen;
            this.generation = generation;
        }

        @Override
        public CompletionStage<Void> complete(CircuitOutcome outcome) {
            Objects.requireNonNull(outcome, "outcome must not be null");
            if (completed.compareAndSet(false, true)) {
                recordOutcome(identity, state, policy, halfOpen, generation, outcome);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private void recordOutcome(
        CircuitIdentity identity,
        CircuitState state,
        CircuitPolicy policy,
        boolean halfOpen,
        long generation,
        CircuitOutcome outcome
    ) {
        CircuitStateTransition transition = null;
        synchronized (state) {
            if (generation != state.generation) {
                return;
            }
            if (halfOpen) {
                state.releaseHalfOpenPermit();
                if (outcome == CircuitOutcome.HEALTH_FAILURE) {
                    state.open(clock.instant().plus(policy.openDuration()));
                    transition = CircuitStateTransition.HALF_OPEN_TO_OPEN;
                } else if (outcome == CircuitOutcome.SUCCESS) {
                    state.close();
                    transition = CircuitStateTransition.HALF_OPEN_TO_CLOSED;
                }
            } else if (outcome == CircuitOutcome.HEALTH_FAILURE && state.status == Status.CLOSED) {
                Instant now = clock.instant();
                state.failures.addLast(now);
                state.discardFailuresBefore(now.minus(policy.failureWindow()));
                if (state.failures.size() >= policy.failureThreshold()) {
                    state.open(now.plus(policy.openDuration()));
                    transition = CircuitStateTransition.CLOSED_TO_OPEN;
                }
            }
        }
        notifyTransition(identity, policy, transition);
    }

    private void notifyTransition(
        CircuitIdentity identity,
        CircuitPolicy policy,
        CircuitStateTransition transition
    ) {
        if (transition == null) {
            return;
        }
        try {
            listener.onTransition(identity, policy.requiredScope(), transition);
        } catch (RuntimeException ignored) {
            // Telemetry must never alter dependency invocation semantics.
        }
    }

    private enum Status {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private static final class CircuitState {
        private final Deque<Instant> failures = new ArrayDeque<>();
        private Status status = Status.CLOSED;
        private Instant openUntil = Instant.MIN;
        private int halfOpenPermitsInFlight;
        private Instant nextHalfOpenRejection = Instant.MIN;
        private long generation;

        private void discardFailuresBefore(Instant cutoff) {
            while (!failures.isEmpty() && failures.peekFirst().isBefore(cutoff)) {
                failures.removeFirst();
            }
        }

        private void open(Instant until) {
            status = Status.OPEN;
            openUntil = until;
            halfOpenPermitsInFlight = 0;
            nextHalfOpenRejection = until;
            generation++;
        }

        private void enterHalfOpen() {
            status = Status.HALF_OPEN;
            halfOpenPermitsInFlight = 0;
            nextHalfOpenRejection = Instant.MIN;
            generation++;
        }

        private void close() {
            status = Status.CLOSED;
            failures.clear();
            halfOpenPermitsInFlight = 0;
            nextHalfOpenRejection = Instant.MIN;
            generation++;
        }

        private void releaseHalfOpenPermit() {
            if (halfOpenPermitsInFlight > 0) {
                halfOpenPermitsInFlight--;
            }
        }
    }
}
