package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.function.Supplier;

import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

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
        if (pipelineContext != null) {
            PipelineContextHolder.set(pipelineContext);
        } else {
            PipelineContextHolder.clear();
        }
        if (awaitContext != null) {
            AwaitExecutionContextHolder.set(awaitContext);
        } else {
            AwaitExecutionContextHolder.clear();
        }
        return new InvocationContextScope(previousPipeline, previousAwait);
    }

    private final class InvocationContextScope implements AutoCloseable {
        private final PipelineContext previousPipeline;
        private final AwaitExecutionContext previousAwait;

        private InvocationContextScope(PipelineContext previousPipeline, AwaitExecutionContext previousAwait) {
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
