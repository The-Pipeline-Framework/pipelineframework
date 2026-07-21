package org.pipelineframework.runtime.core.resilience;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class InMemoryCircuitBreakerTest {
    private static final CircuitIdentity IDENTITY = new CircuitIdentity("payments-api");
    private static final CircuitPolicy LOCAL_POLICY = new CircuitPolicy(
        CircuitScope.LOCAL_PROCESS,
        1,
        Duration.ofMinutes(1),
        Duration.ofSeconds(10),
        1,
        Duration.ofSeconds(2));

    @Test
    void halfOpenSaturationProvidesStaggeredFutureNotBeforeHints() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        InMemoryCircuitBreaker breaker = new InMemoryCircuitBreaker(clock);

        permit(decision(breaker.acquire(IDENTITY, LOCAL_POLICY))).healthFailure();

        CircuitOpen closedWindow = rejection(decision(breaker.acquire(IDENTITY, LOCAL_POLICY)));
        assertEquals(clock.instant().plusSeconds(10), closedWindow.notBefore());

        clock.advance(Duration.ofSeconds(10));
        CircuitPermit halfOpenPermit = permit(decision(breaker.acquire(IDENTITY, LOCAL_POLICY)));
        CircuitOpen firstSaturated = rejection(decision(breaker.acquire(IDENTITY, LOCAL_POLICY)));
        CircuitOpen secondSaturated = rejection(decision(breaker.acquire(IDENTITY, LOCAL_POLICY)));

        assertEquals(clock.instant().plusSeconds(2), firstSaturated.notBefore());
        assertEquals(clock.instant().plusSeconds(4), secondSaturated.notBefore());

        halfOpenPermit.cancel();
        permit(decision(breaker.acquire(IDENTITY, LOCAL_POLICY))).succeed();
        assertInstanceOf(CircuitDecision.Permitted.class, decision(breaker.acquire(IDENTITY, LOCAL_POLICY)));
    }

    @Test
    void rejectsPoliciesWhoseScopeItCannotGuarantee() {
        InMemoryCircuitBreaker breaker = new InMemoryCircuitBreaker();
        CircuitPolicy sharedPolicy = new CircuitPolicy(
            CircuitScope.SHARED_DEPENDENCY,
            1,
            Duration.ofMinutes(1),
            Duration.ofSeconds(1),
            1,
            Duration.ofSeconds(1));

        CompletableFuture<CircuitDecision> result = breaker.acquire(IDENTITY, sharedPolicy).toCompletableFuture();

        assertTrue(result.isCompletedExceptionally());
        CompletionException failure = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
    }

    @Test
    void lateHalfOpenSuccessCannotCloseAReopenedGeneration() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        InMemoryCircuitBreaker breaker = new InMemoryCircuitBreaker(clock);
        CircuitPolicy twoPermits = policyWithHalfOpenPermits(2);

        permit(decision(breaker.acquire(IDENTITY, twoPermits))).healthFailure();
        clock.advance(Duration.ofSeconds(10));
        CircuitPermit first = permit(decision(breaker.acquire(IDENTITY, twoPermits)));
        CircuitPermit second = permit(decision(breaker.acquire(IDENTITY, twoPermits)));

        first.healthFailure();
        second.succeed();

        CircuitOpen reopened = rejection(decision(breaker.acquire(IDENTITY, twoPermits)));
        assertEquals(clock.instant().plusSeconds(10), reopened.notBefore());
    }

    @Test
    void lateHalfOpenCancellationCannotCorruptAReopenedGeneration() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        InMemoryCircuitBreaker breaker = new InMemoryCircuitBreaker(clock);
        CircuitPolicy twoPermits = policyWithHalfOpenPermits(2);

        permit(decision(breaker.acquire(IDENTITY, twoPermits))).healthFailure();
        clock.advance(Duration.ofSeconds(10));
        CircuitPermit first = permit(decision(breaker.acquire(IDENTITY, twoPermits)));
        CircuitPermit second = permit(decision(breaker.acquire(IDENTITY, twoPermits)));

        first.healthFailure();
        second.cancel();
        second.cancel();

        CircuitOpen reopened = rejection(decision(breaker.acquire(IDENTITY, twoPermits)));
        assertEquals(clock.instant().plusSeconds(10), reopened.notBefore());
    }

    @Test
    void localBreakerInstancesDoNotShareCircuitState() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        InMemoryCircuitBreaker first = new InMemoryCircuitBreaker(clock);
        InMemoryCircuitBreaker second = new InMemoryCircuitBreaker(clock);

        permit(decision(first.acquire(IDENTITY, LOCAL_POLICY))).healthFailure();

        assertInstanceOf(CircuitDecision.Rejected.class, decision(first.acquire(IDENTITY, LOCAL_POLICY)));
        assertInstanceOf(CircuitDecision.Permitted.class, decision(second.acquire(IDENTITY, LOCAL_POLICY)));
    }

    @Test
    void emitsStateTransitionsThroughTheListener() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        List<CircuitStateTransition> transitions = new ArrayList<>();
        InMemoryCircuitBreaker breaker = new InMemoryCircuitBreaker(clock, (identity, scope, transition) ->
            transitions.add(transition));

        permit(decision(breaker.acquire(IDENTITY, LOCAL_POLICY))).healthFailure();
        clock.advance(Duration.ofSeconds(10));
        permit(decision(breaker.acquire(IDENTITY, LOCAL_POLICY))).succeed();

        assertEquals(List.of(
            CircuitStateTransition.CLOSED_TO_OPEN,
            CircuitStateTransition.OPEN_TO_HALF_OPEN,
            CircuitStateTransition.HALF_OPEN_TO_CLOSED), transitions);
    }

    @Test
    void listenerNotificationDoesNotBlockUnrelatedCircuitAdmissions() throws Exception {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-19T10:00:00Z"));
        CountDownLatch notificationEntered = new CountDownLatch(1);
        CountDownLatch releaseNotification = new CountDownLatch(1);
        InMemoryCircuitBreaker breaker = new InMemoryCircuitBreaker(clock, (identity, scope, transition) -> {
            if (identity.equals(IDENTITY) && transition == CircuitStateTransition.CLOSED_TO_OPEN) {
                notificationEntered.countDown();
                try {
                    releaseNotification.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        CircuitPermit failingPermit = permit(decision(breaker.acquire(IDENTITY, LOCAL_POLICY)));
        CompletableFuture<Void> completion = CompletableFuture.runAsync(failingPermit::healthFailure);

        try {
            assertTrue(notificationEntered.await(2, TimeUnit.SECONDS));
            assertInstanceOf(
                CircuitDecision.Permitted.class,
                decision(breaker.acquire(new CircuitIdentity("inventory-api"), LOCAL_POLICY)));
        } finally {
            releaseNotification.countDown();
        }
        completion.get(2, TimeUnit.SECONDS);
    }

    private static CircuitPolicy policyWithHalfOpenPermits(int permits) {
        return new CircuitPolicy(
            CircuitScope.LOCAL_PROCESS,
            1,
            Duration.ofMinutes(1),
            Duration.ofSeconds(10),
            permits,
            Duration.ofSeconds(2));
    }

    private static CircuitPermit permit(CircuitDecision decision) {
        return ((CircuitDecision.Permitted) decision).permit();
    }

    private static CircuitDecision decision(java.util.concurrent.CompletionStage<CircuitDecision> decision) {
        return decision.toCompletableFuture().join();
    }

    private static CircuitOpen rejection(CircuitDecision decision) {
        return ((CircuitDecision.Rejected) decision).open();
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }
    }
}
