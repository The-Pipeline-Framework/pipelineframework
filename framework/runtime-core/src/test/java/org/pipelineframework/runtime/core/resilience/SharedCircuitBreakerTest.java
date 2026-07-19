package org.pipelineframework.runtime.core.resilience;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class SharedCircuitBreakerTest {
    private static final CircuitIdentity IDENTITY = new CircuitIdentity("payments");
    private static final CircuitPolicy POLICY = new CircuitPolicy(
        CircuitScope.SHARED_DEPENDENCY, 1, Duration.ofMinutes(1), Duration.ofSeconds(10), 1,
        Duration.ofSeconds(1), Duration.ofSeconds(30));

    @Test
    void freshClosedSnapshotAvoidsPerCallStoreReadsButOpenBecomesVisibleAfterStaleness() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        FakeStore store = new FakeStore(clock);
        SharedCircuitBreaker first = breaker(store, clock);
        SharedCircuitBreaker second = breaker(store, clock);

        CircuitPermit firstPermit = permit(first.acquire(IDENTITY, POLICY));
        assertEquals(1, store.reads);
        assertInstanceOf(CircuitDecision.Permitted.class, decision(first.acquire(IDENTITY, POLICY)));
        assertEquals(1, store.reads);
        assertInstanceOf(CircuitDecision.Permitted.class, decision(second.acquire(IDENTITY, POLICY)));
        assertEquals(2, store.reads);

        firstPermit.healthFailure().toCompletableFuture().join();
        assertInstanceOf(CircuitDecision.Permitted.class, decision(second.acquire(IDENTITY, POLICY)));
        clock.advance(Duration.ofSeconds(2));
        assertInstanceOf(CircuitDecision.Rejected.class, decision(second.acquire(IDENTITY, POLICY)));
    }

    @Test
    void halfOpenLeaseIsGloballyBoundedAndSaturationReturnsAUsefulHint() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        FakeStore store = new FakeStore(clock);
        SharedCircuitBreaker first = breaker(store, clock);
        SharedCircuitBreaker second = breaker(store, clock);

        permit(first.acquire(IDENTITY, POLICY)).healthFailure().toCompletableFuture().join();
        clock.advance(Duration.ofSeconds(11));
        assertInstanceOf(CircuitDecision.Permitted.class, decision(first.acquire(IDENTITY, POLICY)));
        CircuitOpen saturated = ((CircuitDecision.Rejected) decision(second.acquire(IDENTITY, POLICY))).open();

        assertEquals(clock.instant().plusSeconds(30), saturated.notBefore());
    }

    private static SharedCircuitBreaker breaker(FakeStore store, Clock clock) {
        return new SharedCircuitBreaker(store, clock, Duration.ofSeconds(1), Duration.ofSeconds(2),
            CircuitBreakerListener.NOOP);
    }

    private static CircuitDecision decision(CompletionStage<CircuitDecision> stage) {
        return stage.toCompletableFuture().join();
    }

    private static CircuitPermit permit(CompletionStage<CircuitDecision> stage) {
        return ((CircuitDecision.Permitted) decision(stage)).permit();
    }

    private static final class FakeStore implements SharedCircuitStateStore {
        private final MutableClock clock;
        private SharedCircuitSnapshot state = SharedCircuitSnapshot.closed();
        private final Map<Integer, SharedCircuitProbe> probes = new HashMap<>();
        private int reads;

        private FakeStore(MutableClock clock) {
            this.clock = clock;
        }

        @Override
        public synchronized CompletionStage<SharedCircuitSnapshot> read(CircuitIdentity identity, CircuitPolicy policy) {
            reads++;
            return CompletableFuture.completedFuture(state);
        }

        @Override
        public synchronized CompletionStage<SharedCircuitSnapshot> recordHealthFailures(
            CircuitIdentity identity, CircuitPolicy policy, int failures, Instant observedAt) {
            state = new SharedCircuitSnapshot(SharedCircuitStatus.OPEN, state.epoch() + 1, state.version() + 1,
                observedAt.plus(policy.openDuration()));
            return CompletableFuture.completedFuture(state);
        }

        @Override
        public synchronized CompletionStage<SharedCircuitProbeDecision> acquireHalfOpenProbe(
            CircuitIdentity identity, CircuitPolicy policy, String owner, Instant now) {
            if (state.status() == SharedCircuitStatus.OPEN && now.isBefore(state.openUntil())) {
                return CompletableFuture.completedFuture(new SharedCircuitProbeDecision(state, Optional.empty(), state.openUntil()));
            }
            if (state.status() == SharedCircuitStatus.OPEN) {
                state = new SharedCircuitSnapshot(SharedCircuitStatus.HALF_OPEN, state.epoch() + 1,
                    state.version() + 1, Instant.MIN);
            }
            for (int slot = 0; slot < policy.halfOpenMaxPermits(); slot++) {
                SharedCircuitProbe existing = probes.get(slot);
                if (existing == null || !existing.expiresAt().isAfter(now)) {
                    SharedCircuitProbe probe = new SharedCircuitProbe(state.epoch(), slot, UUID.randomUUID().toString(),
                        now.plus(policy.halfOpenProbeLeaseDuration()));
                    probes.put(slot, probe);
                    return CompletableFuture.completedFuture(new SharedCircuitProbeDecision(state, Optional.of(probe), now));
                }
            }
            Instant earliest = probes.values().stream().map(SharedCircuitProbe::expiresAt).min(Instant::compareTo).orElseThrow();
            return CompletableFuture.completedFuture(new SharedCircuitProbeDecision(state, Optional.empty(),
                earliest.isAfter(now.plus(policy.halfOpenRetryDelay())) ? earliest : now.plus(policy.halfOpenRetryDelay())));
        }

        @Override
        public synchronized CompletionStage<Void> completeProbe(
            CircuitIdentity identity, CircuitPolicy policy, SharedCircuitProbe probe, CircuitOutcome outcome, Instant completedAt) {
            SharedCircuitProbe current = probes.get(probe.slot());
            if (current != null && current.leaseToken().equals(probe.leaseToken()) && current.epoch() == state.epoch()) {
                probes.remove(probe.slot());
                if (outcome == CircuitOutcome.SUCCESS) {
                    state = new SharedCircuitSnapshot(SharedCircuitStatus.CLOSED, state.epoch() + 1, state.version() + 1, Instant.MIN);
                } else if (outcome == CircuitOutcome.HEALTH_FAILURE) {
                    state = new SharedCircuitSnapshot(SharedCircuitStatus.OPEN, state.epoch() + 1, state.version() + 1,
                        completedAt.plus(policy.openDuration()));
                }
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant; }
        private void advance(Duration duration) { instant = instant.plus(duration); }
    }
}
