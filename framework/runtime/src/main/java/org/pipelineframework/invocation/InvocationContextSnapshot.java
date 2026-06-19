package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.function.Supplier;

import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

final class InvocationContextSnapshot {
    private final PipelineContext pipelineContext;
    private final AwaitExecutionContext awaitContext;

    InvocationContextSnapshot(PipelineContext pipelineContext, AwaitExecutionContext awaitContext) {
        this.pipelineContext = pipelineContext;
        this.awaitContext = awaitContext;
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
        PipelineExecutionContext previousExecution = PipelineExecutionContextHolder.get();
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
        return new InvocationContextScope(previousPipeline, previousAwait, previousExecution);
    }

    private final class InvocationContextScope implements AutoCloseable {
        private final PipelineContext previousPipeline;
        private final AwaitExecutionContext previousAwait;
        private final PipelineExecutionContext previousExecution;

        private InvocationContextScope(
            PipelineContext previousPipeline,
            AwaitExecutionContext previousAwait,
            PipelineExecutionContext previousExecution
        ) {
            this.previousPipeline = previousPipeline;
            this.previousAwait = previousAwait;
            this.previousExecution = previousExecution;
        }

        @Override
        public void close() {
            if (previousAwait != null) {
                AwaitExecutionContextHolder.set(previousAwait);
            } else {
                AwaitExecutionContextHolder.clear();
            }
            if (previousExecution != null) {
                PipelineExecutionContextHolder.set(previousExecution);
            } else {
                PipelineExecutionContextHolder.clear();
            }
            if (previousPipeline != null) {
                PipelineContextHolder.set(previousPipeline);
            } else {
                PipelineContextHolder.clear();
            }
        }
    }
}
