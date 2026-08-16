package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.telemetry.PipelineRunContext;
import org.pipelineframework.telemetry.PipelineRunContextHolder;

final class InvocationContextSnapshot {
    private final PipelineContext pipelineContext;
    private final AwaitExecutionContext awaitContext;
    private final Optional<PipelineRunContext> runContext;
    private final boolean inheritRunContext;

    InvocationContextSnapshot(PipelineContext pipelineContext, AwaitExecutionContext awaitContext) {
        this(pipelineContext, awaitContext, Optional.empty(), true);
    }

    InvocationContextSnapshot(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Optional<PipelineRunContext> runContext
    ) {
        this(pipelineContext, awaitContext, runContext, false);
    }

    InvocationContextSnapshot(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Optional<PipelineRunContext> runContext,
        boolean inheritRunContext
    ) {
        this.pipelineContext = pipelineContext;
        this.awaitContext = awaitContext;
        this.runContext = Objects.requireNonNull(runContext, "runContext must not be null");
        this.inheritRunContext = inheritRunContext;
    }

    <T> T call(Supplier<T> supplier) {
        Objects.requireNonNull(supplier, "supplier must not be null");
        InvocationContextScope scope = install();
        try {
            return supplier.get();
        } finally {
            scope.close();
        }
    }

    void run(Runnable runnable) {
        Objects.requireNonNull(runnable, "runnable must not be null");
        InvocationContextScope scope = install();
        try {
            runnable.run();
        } finally {
            scope.close();
        }
    }

    private InvocationContextScope install() {
        PipelineContext previousPipeline = PipelineContextHolder.get();
        AwaitExecutionContext previousAwait = AwaitExecutionContextHolder.get();
        Optional<PipelineExecutionContext> previousExecution = PipelineExecutionContextHolder.get();
        Optional<PipelineRunContext> previousRun = PipelineRunContextHolder.get();
        if (pipelineContext != null) {
            PipelineContextHolder.set(pipelineContext);
        } else {
            PipelineContextHolder.clear();
        }
        if (awaitContext != null) {
            AwaitExecutionContextHolder.set(awaitContext);
            PipelineExecutionContextHolder.set(new PipelineExecutionContext(
                awaitContext.tenantId(),
                awaitContext.executionId(),
                awaitContext.currentStepIndex()));
        } else {
            AwaitExecutionContextHolder.clear();
            PipelineExecutionContextHolder.clear();
        }
        if (!inheritRunContext) {
            runContext.ifPresentOrElse(PipelineRunContextHolder::set, PipelineRunContextHolder::clear);
        }
        return new InvocationContextScope(previousPipeline, previousAwait, previousExecution, previousRun);
    }

    private final class InvocationContextScope implements AutoCloseable {
        private final PipelineContext previousPipeline;
        private final AwaitExecutionContext previousAwait;
        private final Optional<PipelineExecutionContext> previousExecution;
        private final Optional<PipelineRunContext> previousRun;

        private InvocationContextScope(
            PipelineContext previousPipeline,
            AwaitExecutionContext previousAwait,
            Optional<PipelineExecutionContext> previousExecution,
            Optional<PipelineRunContext> previousRun
        ) {
            this.previousPipeline = previousPipeline;
            this.previousAwait = previousAwait;
            this.previousExecution = Objects.requireNonNull(previousExecution, "previousExecution must not be null");
            this.previousRun = Objects.requireNonNull(previousRun, "previousRun must not be null");
        }

        @Override
        public void close() {
            previousRun.ifPresentOrElse(PipelineRunContextHolder::set, PipelineRunContextHolder::clear);
            if (previousAwait != null) {
                AwaitExecutionContextHolder.set(previousAwait);
            } else {
                AwaitExecutionContextHolder.clear();
            }
            previousExecution.ifPresentOrElse(PipelineExecutionContextHolder::set, PipelineExecutionContextHolder::clear);
            if (previousPipeline != null) {
                PipelineContextHolder.set(previousPipeline);
            } else {
                PipelineContextHolder.clear();
            }
        }
    }
}
