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
import org.pipelineframework.step.Configurable;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.telemetry.PipelineTelemetry;

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
    PipelineTelemetry telemetry;

    @Inject
    PipelineStepOrderer stepOrderer;

    @Inject
    PipelineParallelismPolicyResolver parallelismPolicyResolver;

    @Inject
    PipelineCacheSupportFactory cacheSupportFactory;

    @Inject
    PipelineStepExecutor stepExecutor;

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
        Objects.requireNonNull(steps, "Steps list must not be null");
        if (!(input instanceof Uni<?> || input instanceof Multi<?>)) {
            throw new IllegalArgumentException(MessageFormat.format(
                "Unsupported input type for PipelineRunner: {0}",
                input == null ? "null" : input.getClass().getName()));
        }

        List<Object> orderedSteps = stepOrderer.orderSteps(steps);

        Object current = input;
        ParallelismPolicy parallelismPolicy = parallelismPolicyResolver.resolveParallelismPolicy(pipelineConfig);
        int maxConcurrency = parallelismPolicyResolver.resolveMaxConcurrency(pipelineConfig);
        PipelineTelemetry.RunContext telemetryContext =
            telemetry.startRun(current, orderedSteps.size(), parallelismPolicy, maxConcurrency);
        current = telemetry.instrumentInput(current, telemetryContext);

        PipelineContext contextSnapshot = PipelineContextHolder.get();
        CacheReadSupport cacheReadSupport = cacheSupportFactory.buildCacheReadSupport();
        for (Object step : orderedSteps) {
            if (step == null) {
                logger.warn("Warning: Found null step in configuration, skipping...");
                continue;
            }

            if (step instanceof Configurable configurable) {
                configurable.initialiseWithConfig(configFactory.buildConfig(step.getClass(), pipelineConfig));
            }

            Class<?> clazz = step.getClass();
            logger.debugf("Step class: %s", clazz.getName());
            for (Class<?> iface : clazz.getInterfaces()) {
                logger.debugf("Implements: %s", iface.getName());
            }

            current = stepExecutor.applyStep(
                step,
                current,
                parallelismPolicy,
                maxConcurrency,
                telemetry,
                telemetryContext,
                cacheReadSupport,
                contextSnapshot);
        }

        return telemetry.instrumentRunCompletion(current, telemetryContext);
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
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
    }

    /**
     * Performs no action; PipelineRunner has no resources to release on close.
     */
    @Override
    public void close() {
    }
}
