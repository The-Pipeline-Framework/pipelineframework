package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.command.CommandRetryExecutionScope;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.telemetry.PipelineRunContext;
import org.pipelineframework.telemetry.PipelineRunContextHolder;

final class InvocationContextSnapshot {
    private final PipelineContext pipelineContext;
    private final AwaitExecutionContext awaitContext;
    private final Optional<PipelineExecutionContext> executionContext;
    private final Optional<PipelineRunContext> runContext;
    private final boolean inheritRunContext;
    private final Optional<PipelineInvocationContext> invocationContext;
    private final CommandRetryExecutionScope.Snapshot commandRetryScope;

    InvocationContextSnapshot(PipelineContext pipelineContext, AwaitExecutionContext awaitContext) {
        this(pipelineContext, awaitContext, Optional.empty(), true, Optional.empty());
    }

    InvocationContextSnapshot(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Optional<PipelineRunContext> runContext
    ) {
        this(pipelineContext, awaitContext, runContext, false, Optional.empty());
    }

    InvocationContextSnapshot(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Optional<PipelineRunContext> runContext,
        boolean inheritRunContext,
        Optional<PipelineInvocationContext> invocationContext
    ) {
        this(
            pipelineContext,
            awaitContext,
            Optional.empty(),
            runContext,
            inheritRunContext,
            invocationContext);
    }

    InvocationContextSnapshot(
        PipelineContext pipelineContext,
        AwaitExecutionContext awaitContext,
        Optional<PipelineExecutionContext> executionContext,
        Optional<PipelineRunContext> runContext,
        boolean inheritRunContext,
        Optional<PipelineInvocationContext> invocationContext
    ) {
        this.pipelineContext = pipelineContext;
        this.awaitContext = awaitContext;
        this.executionContext = Objects.requireNonNull(executionContext, "executionContext must not be null");
        this.runContext = Objects.requireNonNull(runContext, "runContext must not be null");
        this.inheritRunContext = inheritRunContext;
        this.invocationContext = Objects.requireNonNull(invocationContext, "invocationContext must not be null");
        this.commandRetryScope = CommandRetryExecutionScope.capture();
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
        Optional<PipelineInvocationContext> previousInvocation = PipelineInvocationContextHolder.get();
        CommandRetryExecutionScope.Snapshot previousCommandRetry = CommandRetryExecutionScope.capture();
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
        Optional<PipelineExecutionContext> resolvedExecution = executionContext
            .map(context -> awaitContext == null ? context : context.atStep(awaitContext.currentStepIndex()));
        if (resolvedExecution.isEmpty() && awaitContext != null) {
            resolvedExecution = Optional.of(new PipelineExecutionContext(
                awaitContext.tenantId(), awaitContext.executionId(), awaitContext.currentStepIndex()));
        }
        resolvedExecution.ifPresentOrElse(
            PipelineExecutionContextHolder::set,
            PipelineExecutionContextHolder::clear);
        if (!inheritRunContext) {
            runContext.ifPresentOrElse(PipelineRunContextHolder::set, PipelineRunContextHolder::clear);
        }
        invocationContext.ifPresentOrElse(PipelineInvocationContextHolder::set, PipelineInvocationContextHolder::clear);
        CommandRetryExecutionScope.restore(commandRetryScope);
        return new InvocationContextScope(
            previousPipeline, previousAwait, previousExecution, previousRun, previousInvocation, previousCommandRetry);
    }

    private final class InvocationContextScope implements AutoCloseable {
        private final PipelineContext previousPipeline;
        private final AwaitExecutionContext previousAwait;
        private final Optional<PipelineExecutionContext> previousExecution;
        private final Optional<PipelineRunContext> previousRun;
        private final Optional<PipelineInvocationContext> previousInvocation;
        private final CommandRetryExecutionScope.Snapshot previousCommandRetry;

        private InvocationContextScope(
            PipelineContext previousPipeline,
            AwaitExecutionContext previousAwait,
            Optional<PipelineExecutionContext> previousExecution,
            Optional<PipelineRunContext> previousRun,
            Optional<PipelineInvocationContext> previousInvocation,
            CommandRetryExecutionScope.Snapshot previousCommandRetry
        ) {
            this.previousPipeline = previousPipeline;
            this.previousAwait = previousAwait;
            this.previousExecution = Objects.requireNonNull(previousExecution, "previousExecution must not be null");
            this.previousRun = Objects.requireNonNull(previousRun, "previousRun must not be null");
            this.previousInvocation = Objects.requireNonNull(previousInvocation, "previousInvocation must not be null");
            this.previousCommandRetry = Objects.requireNonNull(previousCommandRetry, "previousCommandRetry must not be null");
        }

        @Override
        public void close() {
            previousRun.ifPresentOrElse(PipelineRunContextHolder::set, PipelineRunContextHolder::clear);
            previousInvocation.ifPresentOrElse(
                PipelineInvocationContextHolder::set, PipelineInvocationContextHolder::clear);
            CommandRetryExecutionScope.restore(previousCommandRetry);
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
