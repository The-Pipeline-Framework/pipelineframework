package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.runtime.core.resilience.CircuitBreaker;
import org.pipelineframework.runtime.core.resilience.CircuitDecision;
import org.pipelineframework.runtime.core.resilience.CircuitOpenException;
import org.pipelineframework.runtime.core.resilience.CircuitOutcome;
import org.pipelineframework.runtime.core.resilience.CircuitPermit;
import org.pipelineframework.runtime.core.resilience.InMemoryCircuitBreaker;

/**
 * Shared runtime wrapper for pipeline step and transition worker invocations.
 */
@ApplicationScoped
public class PipelineInvocationRuntime {
    private final CircuitBreaker circuitBreaker;
    private final CircuitPolicyResolver circuitPolicyResolver;
    private final CircuitTelemetry circuitTelemetry;
    private final TransportBoundaryFailureClassifier transportBoundaryFailureClassifier;
    private final TransportBoundaryDiagnostics transportBoundaryDiagnostics;

    public PipelineInvocationRuntime() {
        this(new CircuitTelemetry());
    }

    private PipelineInvocationRuntime(CircuitTelemetry circuitTelemetry) {
        this(
            new InMemoryCircuitBreaker(java.time.Clock.systemUTC(), circuitTelemetry),
            CircuitPolicyResolver.disabled(),
            new TransportBoundaryDiagnostics(),
            circuitTelemetry);
    }

    @Inject
    PipelineInvocationRuntime(
        CircuitBreaker circuitBreaker,
        CircuitPolicyResolver circuitPolicyResolver,
        CircuitTelemetry circuitTelemetry
    ) {
        this(circuitBreaker, circuitPolicyResolver, new TransportBoundaryDiagnostics(), circuitTelemetry);
    }

    PipelineInvocationRuntime(
        CircuitBreaker circuitBreaker,
        CircuitPolicyResolver circuitPolicyResolver,
        TransportBoundaryDiagnostics transportBoundaryDiagnostics,
        CircuitTelemetry circuitTelemetry
    ) {
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
        this.circuitPolicyResolver = Objects.requireNonNull(
            circuitPolicyResolver,
            "circuitPolicyResolver must not be null");
        this.circuitTelemetry = Objects.requireNonNull(circuitTelemetry, "circuitTelemetry must not be null");
        this.transportBoundaryFailureClassifier = new TransportBoundaryFailureClassifier();
        this.transportBoundaryDiagnostics = Objects.requireNonNull(
            transportBoundaryDiagnostics,
            "transportBoundaryDiagnostics must not be null");
    }

    public <T> Uni<T> invokeStepUni(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Supplier<Uni<T>> supplier
    ) {
        return invokeUni(new StepInvocationStrategy(pipelineContext, awaitContext), supplier);
    }

    public <T> Multi<T> invokeStepMulti(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Supplier<Multi<T>> supplier
    ) {
        return invokeMulti(new StepInvocationStrategy(pipelineContext, awaitContext), supplier);
    }

    public <T> Uni<T> invokeTransitionWorker(
        LongConsumer durationNanosRecorder,
        Supplier<Uni<T>> supplier
    ) {
        return invokeUni(new TransitionWorkerInvocationStrategy(durationNanosRecorder), supplier);
    }

    public <T> Uni<T> invokeTransportUni(
        TransportBoundaryInvocation boundary,
        Supplier<Uni<T>> supplier
    ) {
        return invokeTransportUni(
            boundary,
            new TransportBoundaryInvocationStrategy(boundary, transportBoundaryDiagnostics),
            supplier);
    }

    public <T> Multi<T> invokeTransportMulti(
        TransportBoundaryInvocation boundary,
        Supplier<Multi<T>> supplier
    ) {
        return invokeTransportMulti(
            boundary,
            new TransportBoundaryInvocationStrategy(boundary, transportBoundaryDiagnostics),
            supplier);
    }

    private <T> Uni<T> invokeUni(PipelineInvocationStrategy strategy, Supplier<Uni<T>> supplier) {
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        return Uni.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            InvocationContextSnapshot context =
                new InvocationContextSnapshot(strategy.pipelineContext(), strategy.awaitContext());
            try {
                Uni<T> result = context.call(supplier);
                if (result == null) {
                    IllegalStateException failure = new IllegalStateException(strategy.nullUniMessage());
                    strategy.recordTermination(startNanos, failure, false);
                    return Uni.createFrom().failure(failure);
                }
                return new ContextualUni<>(result, context, strategy, startNanos);
            } catch (Throwable failure) {
                strategy.recordTermination(startNanos, failure, false);
                return Uni.createFrom().failure(failure);
            }
        });
    }

    private <T> Multi<T> invokeMulti(PipelineInvocationStrategy strategy, Supplier<Multi<T>> supplier) {
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        return Multi.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            InvocationContextSnapshot context =
                new InvocationContextSnapshot(strategy.pipelineContext(), strategy.awaitContext());
            try {
                Multi<T> result = context.call(supplier);
                if (result == null) {
                    IllegalStateException failure = new IllegalStateException(strategy.nullMultiMessage());
                    strategy.recordTermination(startNanos, failure, false);
                    return Multi.createFrom().failure(failure);
                }
                return new ContextualMulti<>(result, context, strategy, startNanos);
            } catch (Throwable failure) {
                strategy.recordTermination(startNanos, failure, false);
                return Multi.createFrom().failure(failure);
            }
        });
    }

    private <T> Uni<T> invokeTransportUni(
        TransportBoundaryInvocation boundary,
        TransportBoundaryInvocationStrategy strategy,
        Supplier<Uni<T>> supplier
    ) {
        Objects.requireNonNull(boundary, "boundary must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        return Uni.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            CircuitPermit permit;
            try {
                permit = acquirePermit(boundary);
            } catch (CircuitOpenException rejection) {
                return Uni.createFrom().failure(rejection);
            }
            try {
                Uni<T> result = supplier.get();
                if (result == null) {
                    IllegalStateException failure = new IllegalStateException(strategy.nullUniMessage());
                    recordPermitFailure(permit, failure, false);
                    strategy.recordTermination(startNanos, failure, false);
                    return Uni.createFrom().failure(failure);
                }
                CircuitPermit acquiredPermit = permit;
                return result.onTermination().invoke((item, failure, cancelled) -> {
                    recordPermitTermination(acquiredPermit, failure, cancelled);
                    strategy.recordTermination(startNanos, failure, cancelled);
                });
            } catch (Throwable failure) {
                recordPermitFailure(permit, failure, false);
                strategy.recordTermination(startNanos, failure, false);
                return Uni.createFrom().failure(failure);
            }
        });
    }

    private <T> Multi<T> invokeTransportMulti(
        TransportBoundaryInvocation boundary,
        TransportBoundaryInvocationStrategy strategy,
        Supplier<Multi<T>> supplier
    ) {
        Objects.requireNonNull(boundary, "boundary must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        return Multi.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            CircuitPermit permit;
            try {
                permit = acquirePermit(boundary);
            } catch (CircuitOpenException rejection) {
                return Multi.createFrom().failure(rejection);
            }
            try {
                Multi<T> result = supplier.get();
                if (result == null) {
                    IllegalStateException failure = new IllegalStateException(strategy.nullMultiMessage());
                    recordPermitFailure(permit, failure, false);
                    strategy.recordTermination(startNanos, failure, false);
                    return Multi.createFrom().failure(failure);
                }
                CircuitPermit acquiredPermit = permit;
                return result.onTermination().invoke((failure, cancelled) -> {
                    recordPermitTermination(acquiredPermit, failure, cancelled);
                    strategy.recordTermination(startNanos, failure, cancelled);
                });
            } catch (Throwable failure) {
                recordPermitFailure(permit, failure, false);
                strategy.recordTermination(startNanos, failure, false);
                return Multi.createFrom().failure(failure);
            }
        });
    }

    private CircuitPermit acquirePermit(TransportBoundaryInvocation boundary) {
        TransportBoundaryDescriptor descriptor = boundary.transportBoundary();
        Optional<ResolvedCircuitPolicy> resolved = circuitPolicyResolver.resolve(descriptor);
        if (resolved.isEmpty()) {
            return NoopCircuitPermit.INSTANCE;
        }
        ResolvedCircuitPolicy policy = resolved.orElseThrow();
        CircuitDecision decision = circuitBreaker.acquire(policy.identity(), policy.policy());
        return switch (decision) {
            case CircuitDecision.Permitted permitted -> {
                circuitTelemetry.permitted(descriptor, policy);
                yield new TerminalCircuitPermit(permitted.permit());
            }
            case CircuitDecision.Rejected rejected -> {
                circuitTelemetry.rejected(descriptor, rejected.open());
                transportBoundaryDiagnostics.recordCircuitRejected(descriptor);
                throw new CircuitOpenException(rejected.open());
            }
        };
    }

    private void recordPermitTermination(CircuitPermit permit, Throwable failure, boolean cancelled) {
        if (cancelled) {
            safelyComplete(permit, CircuitOutcome.NEUTRAL);
        } else if (failure == null) {
            safelyComplete(permit, CircuitOutcome.SUCCESS);
        } else {
            recordPermitFailure(permit, failure, false);
        }
    }

    private void recordPermitFailure(CircuitPermit permit, Throwable failure, boolean cancelled) {
        TransportBoundaryFailureCategory category = transportBoundaryFailureClassifier.classify(failure, cancelled);
        CircuitOutcome outcome = switch (category) {
            case TIMEOUT, UNAVAILABLE, REMOTE_SERVER -> CircuitOutcome.HEALTH_FAILURE;
            default -> CircuitOutcome.NEUTRAL;
        };
        safelyComplete(permit, outcome);
    }

    private static void safelyComplete(CircuitPermit permit, CircuitOutcome outcome) {
        try {
            permit.complete(outcome);
        } catch (RuntimeException ignored) {
            // Circuit observation must not replace the transport result.
        }
    }

    private enum NoopCircuitPermit implements CircuitPermit {
        INSTANCE;

        @Override
        public void complete(CircuitOutcome outcome) {
        }
    }

    private static final class TerminalCircuitPermit implements CircuitPermit {
        private final CircuitPermit delegate;
        private final AtomicBoolean completed = new AtomicBoolean();

        private TerminalCircuitPermit(CircuitPermit delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        }

        @Override
        public void complete(CircuitOutcome outcome) {
            Objects.requireNonNull(outcome, "outcome must not be null");
            if (completed.compareAndSet(false, true)) {
                delegate.complete(outcome);
            }
        }
    }
}
