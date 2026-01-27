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
import java.util.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.cache.CachePolicyEnforcer;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ParallelismHints;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.step.*;
import org.pipelineframework.step.blocking.StepOneToManyBlocking;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;
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
    private static final int DEFAULT_MAX_CONCURRENCY = 128;

    private enum StepParallelismType {
        ONE_TO_ONE(false),
        ONE_TO_ONE_FUTURE(false),
        ONE_TO_MANY(true),
        ONE_TO_MANY_BLOCKING(true);

        private final boolean autoCandidate;

        StepParallelismType(boolean autoCandidate) {
            this.autoCandidate = autoCandidate;
        }

        boolean autoCandidate() {
            return autoCandidate;
        }
    }

    @Inject
    ConfigFactory configFactory;

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    PipelineTelemetry telemetry;

    /**
     * Default constructor for PipelineRunner.
     */
    public PipelineRunner() {
    }

    /**
     * Execute the provided pipeline steps against a reactive source.
     *
     * Configurable steps are initialised with configuration built from the injected factories before they are applied.
     *
     * @param input  the source Uni or Multi of items to process; steps may convert this to a Uni or a different Multi
     * @param steps  the list of step instances to apply; must not be null — null entries within the list are skipped
     * @return       a Multi containing the resulting stream of items, or a Uni containing the final single result
     * @throws NullPointerException if {@code steps} is null
     * @throws IllegalArgumentException if {@code input} is not a Uni or Multi
     */
    public Object run(Object input, List<Object> steps) {
        Objects.requireNonNull(steps, "Steps list must not be null");
        if (!(input instanceof Uni<?> || input instanceof Multi<?>)) {
            throw new IllegalArgumentException(MessageFormat.format(
                "Unsupported input type for PipelineRunner: {0}",
                input == null ? "null" : input.getClass().getName()));
        }

        // Order the steps according to the pipeline configuration if available
        List<Object> orderedSteps = orderSteps(steps);

        Object current = input;
        ParallelismPolicy parallelismPolicy = resolveParallelismPolicy();
        int maxConcurrency = resolveMaxConcurrency();
        PipelineTelemetry.RunContext telemetryContext =
            telemetry.startRun(current, orderedSteps.size(), parallelismPolicy, maxConcurrency);
        current = telemetry.instrumentInput(current, telemetryContext);

        for (Object step : orderedSteps) {
            if (step == null) {
                logger.warn("Warning: Found null step in configuration, skipping...");
                continue;
            }

            if (step instanceof Configurable c) {
               c.initialiseWithConfig(configFactory.buildConfig(step.getClass(), pipelineConfig));
            }

            Class<?> clazz = step.getClass();
            logger.debugf("Step class: %s", clazz.getName());
            for (Class<?> iface : clazz.getInterfaces()) {
                logger.debugf("Implements: %s", iface.getName());
            }

            switch (step) {
                case StepOneToOne stepOneToOne -> {
                    boolean parallel = shouldParallelize(stepOneToOne, parallelismPolicy, StepParallelismType.ONE_TO_ONE);
                    current = applyOneToOneUnchecked(
                        stepOneToOne, current, parallel, maxConcurrency, telemetry, telemetryContext);
                }
                case StepOneToOneCompletableFuture stepFuture -> {
                    boolean parallel = shouldParallelize(stepFuture, parallelismPolicy, StepParallelismType.ONE_TO_ONE_FUTURE);
                    current = applyOneToOneFutureUnchecked(
                        stepFuture, current, parallel, maxConcurrency, telemetry, telemetryContext);
                }
                case StepOneToMany stepOneToMany -> {
                    boolean parallel = shouldParallelize(stepOneToMany, parallelismPolicy, StepParallelismType.ONE_TO_MANY);
                    current = applyOneToManyUnchecked(
                        stepOneToMany, current, parallel, maxConcurrency, telemetry, telemetryContext);
                }
                case StepOneToManyBlocking stepOneToManyBlocking -> {
                    boolean parallel = shouldParallelize(stepOneToManyBlocking, parallelismPolicy, StepParallelismType.ONE_TO_MANY_BLOCKING);
                    current = applyOneToManyBlockingUnchecked(
                        stepOneToManyBlocking, current, parallel, maxConcurrency, telemetry, telemetryContext);
                }
                case ManyToOne manyToOne -> current = applyManyToOneUnchecked(manyToOne, current, telemetry, telemetryContext);
                case StepManyToMany manyToMany -> current = applyManyToManyUnchecked(manyToMany, current, telemetry, telemetryContext);
                default -> logger.errorf("Step not recognised: %s", step.getClass().getName());
            }
        }

        return telemetry.instrumentRunCompletion(current, telemetryContext); // could be Uni<?> or Multi<?>
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
     * Determine the execution order of the given pipeline steps using the configured global order.
     *
     * If a global order is not configured or contains no valid entries, the original list is returned.
     * Steps named in the configuration are matched by their fully-qualified class name; names present
     * in the configuration but not found in the provided list are logged and ignored. Any steps not
     * mentioned in the configuration are appended in their original relative order.
     *
     * @param steps the list of step instances to order
     * @return an ordered list of step instances according to the pipeline configuration
     */
    List<Object> orderSteps(List<Object> steps) {
        java.util.Optional<List<String>> resourceOrder =
            org.pipelineframework.config.pipeline.PipelineOrderResourceLoader.loadOrder();
        if (resourceOrder.isEmpty()) {
            throw new IllegalStateException(
                "Pipeline order metadata not found. Ensure META-INF/pipeline/order.json is generated at build time.");
        }
        List<String> filteredPipelineOrder = resourceOrder.get();
        if (filteredPipelineOrder.isEmpty()) {
            throw new IllegalStateException(
                "Pipeline order metadata is empty. Ensure pipeline.yaml defines steps for order generation.");
        }
        return applyConfiguredOrder(steps, filteredPipelineOrder);
    }

    private List<Object> applyConfiguredOrder(List<Object> steps, List<String> filteredPipelineOrder) {
        if (filteredPipelineOrder == null || filteredPipelineOrder.isEmpty()) {
            return steps;
        }

        // If the steps list contains entries not listed in the generated order, preserve the existing order.
        java.util.Set<String> configuredNames = new java.util.HashSet<>(filteredPipelineOrder);
        boolean hasUnconfiguredSteps = steps.stream()
            .map(step -> step != null ? step.getClass().getName() : null)
            .anyMatch(name -> name != null && !configuredNames.contains(name));
        if (hasUnconfiguredSteps) {
            logger.debug("Pipeline order configured, but step list contains unconfigured entries; preserving existing order.");
            return steps;
        }

        // Create a map of class name to step instance for quick lookups
        Map<String, Object> stepMap = new HashMap<>();
        for (Object step : steps) {
            stepMap.put(step.getClass().getName(), step);
        }

        // Build the ordered list based on the pipeline configuration
        List<Object> orderedSteps = new ArrayList<>();
        Set<Object> addedSteps = new HashSet<>();
        for (String stepClassName : filteredPipelineOrder) {
            Object step = stepMap.get(stepClassName);
            if (step != null) {
                orderedSteps.add(step);
                addedSteps.add(step);
            } else {
                logger.warnf("Step class %s was specified in pipeline order but was not found in the available steps", stepClassName);
            }
        }

        // Add any remaining steps that weren't specified in the pipeline order
        for (Object step : steps) {
            if (!addedSteps.contains(step)) {
                logger.debugf("Adding step %s that wasn't specified in pipeline order", step.getClass().getName());
                orderedSteps.add(step);
            }
        }

        return orderedSteps;
    }

    private ParallelismPolicy resolveParallelismPolicy() {
        if (pipelineConfig == null || pipelineConfig.parallelism() == null) {
            return ParallelismPolicy.AUTO;
        }
        return pipelineConfig.parallelism();
    }

    private int resolveMaxConcurrency() {
        int configured = pipelineConfig != null ? pipelineConfig.maxConcurrency() : DEFAULT_MAX_CONCURRENCY;
        if (configured < 1) {
            logger.warnf("Invalid maxConcurrency=%s; using 1", configured);
            return 1;
        }
        return configured;
    }

    private boolean shouldParallelize(Object step, ParallelismPolicy policy, StepParallelismType stepType) {
        OrderingRequirement orderingRequirement = OrderingRequirement.RELAXED;
        ThreadSafety threadSafety = ThreadSafety.SAFE;
        boolean hasHints = false;
        if (step instanceof ParallelismHints hints) {
            orderingRequirement = hints.orderingRequirement();
            threadSafety = hints.threadSafety();
            hasHints = true;
        } else if (step != null) {
            ParallelismHint hint = step.getClass().getAnnotation(ParallelismHint.class);
            if (hint != null) {
                orderingRequirement = hint.ordering();
                threadSafety = hint.threadSafety();
                hasHints = true;
            }
        }

        ParallelismPolicy effectivePolicy = policy == null ? ParallelismPolicy.AUTO : policy;
        String stepName = step != null ? step.getClass().getName() : "unknown";

        if (threadSafety == ThreadSafety.UNSAFE && effectivePolicy != ParallelismPolicy.SEQUENTIAL) {
            throw new IllegalStateException("Step " + stepName + " is not thread-safe; " +
                "set pipeline.parallelism=SEQUENTIAL to proceed.");
        }

        if (orderingRequirement == OrderingRequirement.STRICT_REQUIRED &&
            effectivePolicy != ParallelismPolicy.SEQUENTIAL) {
            throw new IllegalStateException("Step " + stepName + " requires strict ordering; " +
                "set pipeline.parallelism=SEQUENTIAL to proceed.");
        }

        if (effectivePolicy == ParallelismPolicy.SEQUENTIAL) {
            return false;
        }

        if (orderingRequirement == OrderingRequirement.STRICT_ADVISED) {
            if (effectivePolicy == ParallelismPolicy.AUTO) {
                logger.warnf("Step %s advises strict ordering; AUTO will run sequentially. " +
                        "Set pipeline.parallelism=PARALLEL to override.",
                    stepName);
                return false;
            }
            logger.warnf("Step %s advises strict ordering; PARALLEL overrides the advice.", stepName);
        }

        if (effectivePolicy == ParallelismPolicy.PARALLEL) {
            return true;
        }

        if (effectivePolicy == ParallelismPolicy.AUTO && hasHints
            && orderingRequirement == OrderingRequirement.RELAXED
            && threadSafety == ThreadSafety.SAFE) {
            return true;
        }

        return stepType.autoCandidate();
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
        return applyOneToOneUnchecked(step, current, false, DEFAULT_MAX_CONCURRENCY, null, null);
    }

    /**
     * Apply a one-to-one step to a Uni or Multi input with optional parallelism and telemetry.
     *
     * @param step the step to apply
     * @param current the current Uni or Multi
     * @param parallel whether to parallelize Multi processing
     * @param maxConcurrency maximum concurrency when parallelized
     * @param telemetry telemetry helper (nullable)
     * @param telemetryContext telemetry run context (nullable)
     * @param <I> input type
     * @param <O> output type
     * @return transformed Uni or Multi
     */
    @SuppressWarnings({"unchecked"})
    public static <I, O> Object applyOneToOneUnchecked(
        StepOneToOne<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<O> result = step.apply(input)
                .onItem().transformToUni(CachePolicyEnforcer::enforce);
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false);
        } else if (current instanceof Multi<?>) {
            Multi<I> multi = (Multi<I>) current;
            if (telemetry != null) {
                multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
            }
            if (parallel) {
                logger.debugf("Applying step %s (merge)", step.getClass());
                return multi
                    .onItem()
                    .transformToUni(item -> {
                        Uni<O> result = step.apply(Uni.createFrom().item(item))
                            .onItem().transformToUni(CachePolicyEnforcer::enforce);
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true);
                    })
                    .merge(maxConcurrency);
            } else {
                logger.debugf("Applying step %s (concatenate)", step.getClass());
                return multi
                    .onItem()
                    .transformToUni(item -> {
                        Uni<O> result = step.apply(Uni.createFrom().item(item))
                            .onItem().transformToUni(CachePolicyEnforcer::enforce);
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true);
                    })
                    .concatenate();
            }
        } else {
            throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepOneToOne: {0}", current));
        }
    }

    @SuppressWarnings("unchecked")
    private static <I, O> Object applyOneToOneFutureUnchecked(
        StepOneToOneCompletableFuture<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<O> result = step.apply(input);
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false);
        } else if (current instanceof Multi<?>) {
            Multi<I> multi = (Multi<I>) current;
            if (telemetry != null) {
                multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
            }
            if (parallel) {
                return multi
                    .onItem()
                    .transformToUni(item -> {
                        Uni<O> result = step.apply(Uni.createFrom().item(item));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true);
                    })
                    .merge(maxConcurrency);
            } else {
                return multi
                    .onItem()
                    .transformToUni(item -> {
                        Uni<O> result = step.apply(Uni.createFrom().item(item));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true);
                    })
                    .concatenate();
            }
        } else {
            throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepOneToOneCompletableFuture: {0}", current));
        }
    }

    @SuppressWarnings({"unchecked"})
    private static <I, O> Object applyOneToManyBlockingUnchecked(
        StepOneToManyBlocking<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Multi<O> result = step.apply(input);
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, false);
        } else if (current instanceof Multi<?>) {
            if (parallel) {
                logger.debugf("Applying step %s (merge)", step.getClass());
                Multi<I> multi = (Multi<I>) current;
                if (telemetry != null) {
                    multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
                }
                return multi
                    .onItem()
                    .transformToMulti(item -> {
                        Multi<O> result = step.apply(Uni.createFrom().item(item));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true);
                    })
                    .merge(maxConcurrency);
            } else {
                logger.debugf("Applying step %s (concatenate)", step.getClass());
                Multi<I> multi = (Multi<I>) current;
                if (telemetry != null) {
                    multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
                }
                return multi
                    .onItem()
                    .transformToMulti(item -> {
                        Multi<O> result = step.apply(Uni.createFrom().item(item));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true);
                    })
                    .concatenate();
            }
        } else {
            throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepOneToManyBlocking: {0}", current));
        }
    }

    @SuppressWarnings({"unchecked"})
    private static <I, O> Object applyOneToManyUnchecked(
        StepOneToMany<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Multi<O> result = step.apply(input);
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, false);
        } else if (current instanceof Multi<?>) {
            if (parallel) {
                logger.debugf("Applying step %s (merge)", step.getClass());
                Multi<I> multi = (Multi<I>) current;
                if (telemetry != null) {
                    multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
                }
                return multi
                    .onItem()
                    .transformToMulti(item -> {
                        Multi<O> result = step.apply(Uni.createFrom().item(item));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true);
                    })
                    .merge(maxConcurrency);
            } else {
                logger.debugf("Applying step %s (concatenate)", step.getClass());
                Multi<I> multi = (Multi<I>) current;
                if (telemetry != null) {
                    multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
                }
                return multi
                    .onItem()
                    .transformToMulti(item -> {
                        Multi<O> result = step.apply(Uni.createFrom().item(item));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true);
                    })
                    .concatenate();
            }
        } else {
            throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepOneToMany: {0}", current));
        }
    }

    @SuppressWarnings("unchecked")
    private static <I, O> Object applyManyToOneUnchecked(
        ManyToOne<I, O> step,
        Object current,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext) {
        if (current instanceof Multi<?>) {
            Multi<I> input = (Multi<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<O> result = step.apply(input);
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false);
        } else if (current instanceof Uni<?>) {
            // convert Uni to Multi and call apply
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<O> result = step.apply(input.toMulti());
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false);
        } else {
            throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepManyToOne: {0}", current));
        }
    }

    @SuppressWarnings("unchecked")
    private static <I, O> Object applyManyToManyUnchecked(
        StepManyToMany<I, O> step,
        Object current,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext) {
        if (current instanceof Uni<?>) {
            // Single async source — convert to Multi and process
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Multi<O> result = step.apply(input.toMulti());
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, false);
        } else if (current instanceof Multi<?> c) {
            logger.debugf("Applying many-to-many step %s on full stream", step.getClass());
            // ✅ Just pass the whole stream to the step
            Multi<I> input = (Multi<I>) c;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Multi<O> result = step.apply(input);
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, false);
        } else {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Unsupported current type for StepManyToMany: {0}", current));
        }
    }

    /**
     * Performs no action; PipelineRunner has no resources to release on close.
     */
    @Override
    public void close() {
    }
}
