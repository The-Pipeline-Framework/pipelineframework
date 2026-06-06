package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.context.PipelineContext;

/**
 * Shared runtime wrapper for pipeline step and transition worker invocations.
 */
@ApplicationScoped
public class PipelineInvocationRuntime {
    private final TransportBoundaryDiagnostics transportBoundaryDiagnostics;

    public PipelineInvocationRuntime() {
        this(new TransportBoundaryDiagnostics());
    }

    PipelineInvocationRuntime(TransportBoundaryDiagnostics transportBoundaryDiagnostics) {
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
        return invokeUni(new TransportBoundaryInvocationStrategy(boundary, transportBoundaryDiagnostics), supplier);
    }

    public <T> Multi<T> invokeTransportMulti(
        TransportBoundaryInvocation boundary,
        Supplier<Multi<T>> supplier
    ) {
        return invokeMulti(new TransportBoundaryInvocationStrategy(boundary, transportBoundaryDiagnostics), supplier);
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
}
