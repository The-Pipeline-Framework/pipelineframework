package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Shared runtime wrapper for pipeline step and transition worker invocations.
 */
public final class PipelineInvocationRuntime {

    private PipelineInvocationRuntime() {
    }

    public static <T> Uni<T> invokeStepUni(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Supplier<Uni<T>> supplier
    ) {
        return invokeUni(new StepInvocationStrategy(pipelineContext, awaitContext), supplier);
    }

    public static <T> Multi<T> invokeStepMulti(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Supplier<Multi<T>> supplier
    ) {
        return invokeMulti(new StepInvocationStrategy(pipelineContext, awaitContext), supplier);
    }

    public static <T> Uni<T> invokeTransitionWorker(
        LongConsumer durationRecorder,
        Supplier<Uni<T>> supplier
    ) {
        return invokeUni(new TransitionWorkerInvocationStrategy(durationRecorder), supplier);
    }

    private static <T> Uni<T> invokeUni(PipelineInvocationStrategy strategy, Supplier<Uni<T>> supplier) {
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        return Uni.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            ExecutionContextScope scope = installExecutionContexts(strategy.pipelineContext(), strategy.awaitContext());
            try {
                Uni<T> result = supplier.get();
                if (result == null) {
                    return Uni.createFrom().failure(new IllegalStateException(strategy.nullUniMessage()));
                }
                return result.onTermination().invoke(() -> {
                    scope.close();
                    strategy.recordDuration(startNanos);
                });
            } catch (Throwable failure) {
                scope.close();
                strategy.recordDuration(startNanos);
                return Uni.createFrom().failure(failure);
            }
        });
    }

    private static <T> Multi<T> invokeMulti(PipelineInvocationStrategy strategy, Supplier<Multi<T>> supplier) {
        Objects.requireNonNull(strategy, "strategy must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        return Multi.createFrom().deferred(() -> {
            long startNanos = System.nanoTime();
            ExecutionContextScope scope = installExecutionContexts(strategy.pipelineContext(), strategy.awaitContext());
            try {
                Multi<T> result = supplier.get();
                if (result == null) {
                    return Multi.createFrom().failure(new IllegalStateException(strategy.nullMultiMessage()));
                }
                return result.onTermination().invoke((failure, cancelled) -> {
                    scope.close();
                    strategy.recordDuration(startNanos);
                });
            } catch (Throwable failure) {
                scope.close();
                strategy.recordDuration(startNanos);
                return Multi.createFrom().failure(failure);
            }
        });
    }

    private static ExecutionContextScope installExecutionContexts(
        PipelineContext context,
        AwaitExecutionContext awaitContext
    ) {
        PipelineContext previousPipeline = PipelineContextHolder.get();
        AwaitExecutionContext previousAwait = AwaitExecutionContextHolder.get();
        if (context != null) {
            PipelineContextHolder.set(context);
        } else {
            PipelineContextHolder.clear();
        }
        if (awaitContext != null) {
            AwaitExecutionContextHolder.set(awaitContext);
        } else {
            AwaitExecutionContextHolder.clear();
        }
        return new ExecutionContextScope(previousPipeline, previousAwait);
    }

    private static final class ExecutionContextScope implements AutoCloseable {
        private final PipelineContext previousPipeline;
        private final AwaitExecutionContext previousAwait;

        private ExecutionContextScope(PipelineContext previousPipeline, AwaitExecutionContext previousAwait) {
            this.previousPipeline = previousPipeline;
            this.previousAwait = previousAwait;
        }

        @Override
        public void close() {
            if (previousAwait != null) {
                AwaitExecutionContextHolder.set(previousAwait);
            } else {
                AwaitExecutionContextHolder.clear();
            }
            if (previousPipeline != null) {
                PipelineContextHolder.set(previousPipeline);
            } else {
                PipelineContextHolder.clear();
            }
        }
    }
}
