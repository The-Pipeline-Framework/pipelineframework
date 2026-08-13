/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework;

import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.cache.PipelineCacheReader;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.TerminalOutputOwnership;
import org.pipelineframework.objectpublish.ObjectPublishRunner;
import org.pipelineframework.objectpublish.ObjectPublishTelemetry;
import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.telemetry.PipelineRunContext;
import org.pipelineframework.telemetry.PipelineRunTelemetry;
import org.pipelineframework.telemetry.PipelineStepTelemetry;
import org.pipelineframework.telemetry.PipelineTracingSupport;

/**
 * A service that runs a sequence of pipeline steps against a reactive source.
 *
 * This class orchestrates the execution of pipeline steps, handling the transformation of reactive streams
 * through various step types (one-to-one, one-to-many, many-to-one, many-to-many).
 */
@ApplicationScoped
@Unremovable
public class PipelineRunner implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(PipelineRunner.class);
    private static final int DEFAULT_MAX_CONCURRENCY = PipelineParallelismPolicyResolver.DEFAULT_MAX_CONCURRENCY;

    @Inject
    ConfigFactory configFactory;

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    PipelineRunTelemetry runTelemetry;

    @Inject
    PipelineStepTelemetry.Seam stepTelemetry;

    @Inject
    PipelineStepOrderer stepOrderer;

    @Inject
    PipelineParallelismPolicyResolver parallelismPolicyResolver;

    @Inject
    PipelineCacheSupportFactory cacheSupportFactory;

    @Inject
    PipelineStepExecutor stepExecutor;

    @Inject
    Instance<ObjectPublishTelemetry> objectPublishTelemetry;

    private final PipelineRunnerCore runnerCore = new PipelineRunnerCore();
    private volatile ObjectPublishRunner objectPublishRunner;

    /**
     * Default constructor for PipelineRunner.
     */
    public PipelineRunner() {
    }

    /**
     * Run a configured sequence of pipeline steps against a reactive source.
     *
     * The method initializes configurable steps, determines execution order and parallelism, integrates optional
     * cache-read support and telemetry, and applies each step in sequence to transform the input stream.
     *
     * @param input  the reactive source to process; must be a Uni or a Multi
     * @param steps  ordered list of step instances to apply; null entries are skipped
     * @return       the pipeline's final reactive result: a Uni for a single-result pipeline or a Multi for a stream
     * @throws NullPointerException     if {@code steps} is null
     * @throws IllegalArgumentException if {@code input} is not a Uni or a Multi
     */
    public Object run(Object input, List<Object> steps) {
        return runWithContext(input, steps).result();
    }

    public ExecutionResult runWithContext(Object input, List<Object> steps) {
        return runFromStepWithContext(input, steps, 0);
    }

    /**
     * Run a configured sequence starting at a specific ordered step index.
     *
     * @param input reactive source to process
     * @param steps ordered or orderable step instances
     * @param startStepIndex first ordered step index to execute
     * @return final reactive result
     */
    public Object runFromStep(Object input, List<Object> steps, int startStepIndex) {
        return runFromStepWithContext(input, steps, startStepIndex).result();
    }

    public ExecutionResult runFromStepWithContext(Object input, List<Object> steps, int startStepIndex) {
        return runFromStepUntilWithContext(input, steps, startStepIndex, steps == null ? 0 : steps.size());
    }

    public ExecutionResult runFromStepUntilWithContext(
        Object input,
        List<Object> steps,
        int startStepIndex,
        int stopBeforeStepIndex) {
        return runFromStepUntilWithContext(input, steps, startStepIndex, stopBeforeStepIndex, true);
    }

    /**
     * Runs a statically linked child definition within the current root invocation.
     *
     * <p>The child uses the same step executor, configuration, and {@link PipelineContext} capture
     * as a top-level range, but it neither starts another pipeline run nor owns terminal object
     * publication. The caller receives the child reactive result to flatten through its ordinary
     * step interface.
     *
     * @param input child input as a Uni or Multi
     * @param steps statically linked child step instances
     * @return child result without terminal publication ownership
     */
    public ExecutionResult runNestedWithContext(Object input, List<Object> steps) {
        Objects.requireNonNull(steps, "Steps list must not be null");
        return runFromStepUntilWithContext(input, steps, 0, steps.size(), false);
    }

    private ExecutionResult runFromStepUntilWithContext(
        Object input,
        List<Object> steps,
        int startStepIndex,
        int stopBeforeStepIndex,
        boolean rootInvocation) {
        Objects.requireNonNull(steps, "Steps list must not be null");
        if (!(input instanceof Uni<?> || input instanceof Multi<?>)) {
            throw new IllegalArgumentException(MessageFormat.format(
                "Unsupported input type for PipelineRunner: {0}",
                input == null ? "null" : input.getClass().getName()));
        }

        List<Object> orderedSteps = stepOrderer.orderSteps(steps);
        if (startStepIndex < 0 || startStepIndex > orderedSteps.size()) {
            throw new IllegalArgumentException("startStepIndex is out of range: " + startStepIndex);
        }
        if (stopBeforeStepIndex < startStepIndex || stopBeforeStepIndex > orderedSteps.size()) {
            throw new IllegalArgumentException("stopBeforeStepIndex is out of range: " + stopBeforeStepIndex);
        }

        ParallelismPolicy parallelismPolicy = parallelismPolicyResolver.resolveParallelismPolicy(pipelineConfig);
        int maxConcurrency = parallelismPolicyResolver.resolveMaxConcurrency(pipelineConfig);
        PipelineRunContext telemetryContext = rootInvocation
            ? runTelemetry.startRun(input, orderedSteps.size(), parallelismPolicy, maxConcurrency)
            : PipelineRunContext.disabled();
        Object instrumentedInput = rootInvocation ? runTelemetry.instrumentInput(input, telemetryContext) : input;
        PipelineStepTelemetry executionStepTelemetry = rootInvocation
            ? PipelineStepTelemetry.of(stepTelemetry, telemetryContext)
            : PipelineStepTelemetry.disabled();

        PipelineContext contextSnapshot = PipelineContextHolder.get();
        CacheReadSupport cacheReadSupport = cacheSupportFactory.buildCacheReadSupport();
        AwaitExecutionContext awaitContext = AwaitExecutionContextHolder.get();
        Object current = runnerCore.runSync(
            instrumentedInput,
            orderedSteps,
            startStepIndex,
            stopBeforeStepIndex,
            (step, value, index) -> {
                AwaitExecutionContext awaitContextSnapshot = awaitContext == null
                    ? null
                    : new AwaitExecutionContext(
                        awaitContext.tenantId(),
                        awaitContext.executionId(),
                        index,
                        awaitContext.continuationMode(),
                        awaitContext.terminalOutputOwnership(),
                        PipelineTracingSupport.capture(
                            telemetryContext == null || telemetryContext.span() == null
                                ? io.opentelemetry.api.trace.SpanContext.getInvalid()
                                : telemetryContext.span().getSpanContext()));

                if (step instanceof Configurable configurable) {
                    configurable.initialiseWithConfig(configFactory.buildConfig(step.getClass(), pipelineConfig));
                }

                Class<?> clazz = step.getClass();
                logger.debugf("Step class: %s", clazz.getName());
                for (Class<?> iface : clazz.getInterfaces()) {
                    logger.debugf("Implements: %s", iface.getName());
                }

                return stepExecutor.applyStep(
                    step,
                    value,
                    parallelismPolicy,
                    maxConcurrency,
                    executionStepTelemetry,
                    cacheReadSupport,
                    contextSnapshot,
                    awaitContextSnapshot);
            },
            index -> logger.warnf("Warning: Found null step at index %d in configuration, skipping...", index));

        // Terminal object publish only runs after a full pipeline execution, not for partial/early-stop runs.
        Object terminal = current;
        boolean terminalOutputPublished = false;
        if (rootInvocation
            && stopBeforeStepIndex == orderedSteps.size()
            && (awaitContext == null
                || awaitContext.terminalOutputOwnership() == TerminalOutputOwnership.TRANSITION_WORKER)) {
            ObjectPublishRunner publishRunner = objectPublishRunner();
            if (publishRunner.enabled()) {
                terminal = publishRunner.publish(current);
                terminalOutputPublished = true;
            }
        }
        Object completed = rootInvocation ? runTelemetry.instrumentRunCompletion(terminal, telemetryContext) : terminal;
        return new ExecutionResult(
            completed,
            telemetryContext,
            terminalOutputPublished);
    }

    public record ExecutionResult(
        Object result,
        PipelineRunContext telemetryContext,
        boolean terminalOutputPublished) {
        public ExecutionResult(Object result, PipelineRunContext telemetryContext) {
            this(result, telemetryContext, false);
        }
    }

    /**
     * Execute the provided pipeline steps against a reactive source Multi.
     *
     * @param input  the source Multi of items to process; steps may convert this to a Uni or a different Multi
     * @param steps  the list of step instances to apply; must not be null — null entries within the list are skipped
     * @return       a Multi containing the resulting stream of items, or a Uni containing the final single result
     * @throws NullPointerException if {@code steps} is null
     */
    public Object run(Multi<?> input, List<Object> steps) {
        return run((Object) input, steps);
    }

    /**
     * Apply a one-to-one pipeline step to the provided reactive stream and produce the transformed stream.
     *
     * @param <I>     the input type of the step
     * @param <O>     the output type of the step
     * @param step    the step that transforms items of type I to type O
     * @param current a Uni&lt;?&gt; or Multi&lt;?&gt; that provides the input items; other types are not supported
     * @return        the resulting Uni&lt;?&gt; or Multi&lt;?&gt; after applying the step
     * @throws IllegalArgumentException if {@code current} is neither a Uni&lt;?&gt; nor a Multi&lt;?&gt;
     */
    @SuppressWarnings({"unchecked"})
    public static <I, O> Object applyOneToOneUnchecked(StepOneToOne<I, O> step, Object current) {
        return applyOneToOneUnchecked(step, current, false, DEFAULT_MAX_CONCURRENCY, null, null, null, null);
    }

    /**
     * Apply a one-to-one pipeline step to a Uni or Multi, producing a transformed Uni or Multi.
     *
     * <p>Supports optional parallel processing for Multi inputs, max-concurrency control,
     * cache-aware execution, and telemetry instrumentation.</p>
     */
    @SuppressWarnings({"unchecked"})
    public static <I, O> Object applyOneToOneUnchecked(
        StepOneToOne<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        CacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot) {
        return PipelineStepExecutor.applyOneToOneUnchecked(
            step,
            current,
            parallel,
            maxConcurrency,
            telemetry,
            telemetryContext,
            cacheReadSupport,
            contextSnapshot);
    }

    /**
     * Compatibility wrapper for callers that still reference {@code PipelineRunner.CacheReadSupport}.
     *
     * @deprecated Use {@link PipelineCacheReadSupport} directly. The constructor mapping is unchanged:
     * pass the same {@link PipelineCacheReader}, {@link List} of {@link CacheKeyStrategy}, and
     * {@code defaultPolicy}.
     */
    @Deprecated(forRemoval = false)
    static final class CacheReadSupport extends PipelineCacheReadSupport {
        CacheReadSupport(PipelineCacheReader reader, List<CacheKeyStrategy> strategies, String defaultPolicy) {
            super(reader, strategies, defaultPolicy);
        }

        CacheReadSupport(
            PipelineCacheReader reader,
            java.util.Optional<org.pipelineframework.cache.PipelineCacheWriter> writer,
            List<CacheKeyStrategy> strategies,
            String defaultPolicy,
            java.util.Optional<java.time.Duration> configuredTtl
        ) {
            super(reader, writer, strategies, defaultPolicy, configuredTtl);
        }
    }

    /**
     * Performs no action; PipelineRunner has no resources to release on close.
     */
    @Override
    public void close() {
    }

    private ObjectPublishRunner objectPublishRunner() {
        ObjectPublishRunner runner = objectPublishRunner;
        if (runner != null) {
            return runner;
        }
        synchronized (this) {
            runner = objectPublishRunner;
            if (runner == null) {
                runner = ObjectPublishRunner.loadFromDefaultConfig(
                    objectPublishTelemetry != null && objectPublishTelemetry.isResolvable()
                        ? objectPublishTelemetry.get()
                        : ObjectPublishTelemetry.NOOP);
                objectPublishRunner = runner;
            }
            return runner;
        }
    }
}
