package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.ws.rs.ProcessingException;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.resilience.CircuitBreaker;
import org.pipelineframework.runtime.core.resilience.CircuitDecision;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitOpen;
import org.pipelineframework.runtime.core.resilience.CircuitOpenException;
import org.pipelineframework.runtime.core.resilience.CircuitOutcome;
import org.pipelineframework.runtime.core.resilience.CircuitPolicy;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

class PipelineInvocationCircuitLifecycleTest {
    private static final TransportBoundaryInvocation BOUNDARY =
        () -> new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess");

    @Test
    void recordsOneOutcomeForSuccessAndSynchronousAndAsynchronousFailures() {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        PipelineInvocationRuntime runtime = runtime(breaker);

        assertEquals("ok", runtime.invokeTransportUni(BOUNDARY, () -> Uni.createFrom().item("ok"))
            .await().indefinitely());
        assertThrows(IllegalArgumentException.class, () -> runtime.invokeTransportUni(BOUNDARY, () -> {
            throw new IllegalArgumentException("validation");
        }).await().indefinitely());
        assertThrows(ProcessingException.class, () -> runtime.invokeTransportUni(
            BOUNDARY,
            () -> Uni.createFrom().failure(new ProcessingException("down"))).await().indefinitely());

        assertEquals(List.of(
            CircuitOutcome.SUCCESS,
            CircuitOutcome.NEUTRAL,
            CircuitOutcome.HEALTH_FAILURE), breaker.outcomes());
    }

    @Test
    void cancellationAndMultiTerminationReleaseExactlyOneNeutralOutcome() throws InterruptedException {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        PipelineInvocationRuntime runtime = runtime(breaker);
        CountDownLatch cancelled = new CountDownLatch(1);
        Multi<String> stream = Multi.createFrom().emitter(emitter -> {
            emitter.emit("first");
            emitter.onTermination(cancelled::countDown);
        });

        AssertSubscriber<String> subscriber = runtime.invokeTransportMulti(BOUNDARY, () -> stream)
            .subscribe().withSubscriber(AssertSubscriber.create(1));
        subscriber.awaitItems(1, Duration.ofSeconds(2));
        subscriber.cancel();

        assertEquals(true, cancelled.await(2, TimeUnit.SECONDS));
        assertEquals(List.of(CircuitOutcome.NEUTRAL), breaker.outcomes());
    }

    @Test
    void multiCompletionAndFailureUseTheSameCircuitOutcomeRules() {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        PipelineInvocationRuntime runtime = runtime(breaker);

        assertEquals(List.of("first", "second"), runtime.invokeTransportMulti(
            BOUNDARY,
            () -> Multi.createFrom().items("first", "second")).collect().asList().await().indefinitely());
        assertThrows(ProcessingException.class, () -> runtime.invokeTransportMulti(
            BOUNDARY,
            () -> Multi.createFrom().failure(new ProcessingException("down"))).collect().asList().await().indefinitely());

        assertEquals(List.of(CircuitOutcome.SUCCESS, CircuitOutcome.HEALTH_FAILURE), breaker.outcomes());
    }

    @Test
    void retryResubscriptionAcquiresAndCompletesAFreshPermit() {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        PipelineInvocationRuntime runtime = runtime(breaker);
        AtomicInteger suppliers = new AtomicInteger();

        assertThrows(ProcessingException.class, () -> runtime.invokeTransportUni(BOUNDARY, () -> {
            suppliers.incrementAndGet();
            return Uni.createFrom().failure(new ProcessingException("down"));
        }).onFailure().retry().atMost(2).await().indefinitely());

        assertEquals(3, suppliers.get());
        assertEquals(3, breaker.acquisitions());
        assertEquals(List.of(
            CircuitOutcome.HEALTH_FAILURE,
            CircuitOutcome.HEALTH_FAILURE,
            CircuitOutcome.HEALTH_FAILURE), breaker.outcomes());
    }

    @Test
    void supplierThrownCircuitOpenCompletesItsAcquiredPermitAsNeutral() {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        PipelineInvocationRuntime runtime = runtime(breaker);
        CircuitOpenException supplierFailure = new CircuitOpenException(new CircuitOpen(
            new CircuitIdentity("downstream"), CircuitScope.LOCAL_PROCESS, java.time.Instant.now()));

        assertThrows(CircuitOpenException.class, () -> runtime.invokeTransportMulti(BOUNDARY, () -> {
            throw supplierFailure;
        }).collect().asList().await().indefinitely());

        assertEquals(List.of(CircuitOutcome.NEUTRAL), breaker.outcomes());
    }

    private static PipelineInvocationRuntime runtime(RecordingCircuitBreaker breaker) {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", new CircuitSettings(
                true,
                CircuitScope.LOCAL_PROCESS,
                1,
                Duration.ofMinutes(1),
                Duration.ofSeconds(30),
                1,
                Duration.ofSeconds(1),
                Optional.empty())));
        return new PipelineInvocationRuntime(
            breaker,
            resolver,
            new TransportBoundaryDiagnostics(),
            new CircuitTelemetry(io.opentelemetry.api.OpenTelemetry.noop()
                .getMeter("org.pipelineframework.resilience")));
    }

    private static final class RecordingCircuitBreaker implements CircuitBreaker {
        private final AtomicInteger acquisitions = new AtomicInteger();
        private final List<CircuitOutcome> outcomes = new ArrayList<>();

        @Override
        public CircuitDecision acquire(CircuitIdentity identity, CircuitPolicy policy) {
            acquisitions.incrementAndGet();
            return new CircuitDecision.Permitted(outcome -> outcomes.add(outcome));
        }

        private int acquisitions() {
            return acquisitions.get();
        }

        private List<CircuitOutcome> outcomes() {
            return List.copyOf(outcomes);
        }
    }
}
