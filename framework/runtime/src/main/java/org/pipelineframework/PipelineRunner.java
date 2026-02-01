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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.cache.*;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
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

    @Inject
    Instance<CacheKeyStrategy> cacheKeyStrategies;

    @Inject
    Instance<PipelineCacheReader> cacheReaders;

    @ConfigProperty(name = "pipeline.cache.policy", defaultValue = "prefer-cache")
    String cachePolicyDefault;

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

        // Order the steps according to the pipeline configuration if available
        List<Object> orderedSteps = orderSteps(steps);

        Object current = input;
        ParallelismPolicy parallelismPolicy = resolveParallelismPolicy();
        int maxConcurrency = resolveMaxConcurrency();
        PipelineTelemetry.RunContext telemetryContext =
            telemetry.startRun(current, orderedSteps.size(), parallelismPolicy, maxConcurrency);
        current = telemetry.instrumentInput(current, telemetryContext);

        PipelineContext contextSnapshot = PipelineContextHolder.get();
        CacheReadSupport cacheReadSupport = buildCacheReadSupport();
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
                        stepOneToOne, current, parallel, maxConcurrency, telemetry, telemetryContext, cacheReadSupport, contextSnapshot);
                }
                case StepOneToOneCompletableFuture stepFuture -> {
                    boolean parallel = shouldParallelize(stepFuture, parallelismPolicy, StepParallelismType.ONE_TO_ONE_FUTURE);
                    current = applyOneToOneFutureUnchecked(
                        stepFuture, current, parallel, maxConcurrency, telemetry, telemetryContext, contextSnapshot);
                }
                case StepOneToMany stepOneToMany -> {
                    boolean parallel = shouldParallelize(stepOneToMany, parallelismPolicy, StepParallelismType.ONE_TO_MANY);
                    current = applyOneToManyUnchecked(
                        stepOneToMany, current, parallel, maxConcurrency, telemetry, telemetryContext, contextSnapshot);
                }
                case StepOneToManyBlocking stepOneToManyBlocking -> {
                    boolean parallel = shouldParallelize(stepOneToManyBlocking, parallelismPolicy, StepParallelismType.ONE_TO_MANY_BLOCKING);
                    current = applyOneToManyBlockingUnchecked(
                        stepOneToManyBlocking, current, parallel, maxConcurrency, telemetry, telemetryContext, contextSnapshot);
                }
                case ManyToOne manyToOne -> current = applyManyToOneUnchecked(manyToOne, current, telemetry, telemetryContext, contextSnapshot);
                case StepManyToMany manyToMany -> current = applyManyToManyUnchecked(manyToMany, current, telemetry, telemetryContext, contextSnapshot);
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

    /**
     * Determine whether a pipeline step should be executed in parallel.
     *
     * Considers explicit hints provided by the step (ParallelismHints or @ParallelismHint),
     * the effective pipeline-level policy, and the step's parallelism category; falls back
     * to the stepType's autoCandidate flag when hints and policy do not decide.
     *
     * @param step the step instance (may implement ParallelismHints or be annotated with @ParallelismHint); may be null
     * @param policy the configured pipeline parallelism policy (may be null, treated as AUTO)
     * @param stepType the step's parallelism category used as a fallback auto-candidate hint
     * @return `true` if the step should be parallelized, `false` otherwise
     * @throws IllegalStateException if the step is not thread-safe or requires strict ordering while the effective policy is not SEQUENTIAL
     */
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
     * Constructs a CacheReadSupport instance when a cache reader and at least one cache key strategy are available.
     *
     * @return a CacheReadSupport configured with the first available PipelineCacheReader, the list of CacheKeyStrategy
     *         instances ordered by descending priority, and the configured default cache policy; returns `null` if no
     *         cache reader, no cache key strategies, or other required components are present
     */
    private CacheReadSupport buildCacheReadSupport() {
        if (cacheReaders == null) {
            return null;
        }
        PipelineCacheReader reader = cacheReaders.stream().findFirst().orElse(null);
        if (reader == null || cacheKeyStrategies == null) {
            return null;
        }
        List<CacheKeyStrategy> ordered = cacheKeyStrategies.stream()
            .sorted(Comparator.comparingInt(CacheKeyStrategy::priority).reversed())
            .toList();
        if (ordered.isEmpty()) {
            return null;
        }
        return new CacheReadSupport(reader, ordered, cachePolicyDefault);
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
     *
     * @param step the step to apply
     * @param current the current reactive stream (a {@code Uni} or {@code Multi}) to transform
     * @param parallel whether to process Multi items in parallel
     * @param maxConcurrency maximum number of concurrent inner subscriptions when parallel is {@code true}
     * @param telemetry telemetry helper; may be {@code null}
     * @param telemetryContext telemetry run context; may be {@code null}
     * @param cacheReadSupport cache read support used for cache-aware execution; may be {@code null}
     * @param contextSnapshot pipeline context snapshot used during step execution; may be {@code null}
     * @param <I> input element type
     * @param <O> output element type
     * @return a {@code Uni<O>} or {@code Multi<O>} representing the transformed stream
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
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<O> result = input
                .onItem()
                .transformToUni(item -> applyOneToOneWithCache(step, item, cacheReadSupport, contextSnapshot))
                .onItem()
                .transformToUni(item -> withPipelineContext(contextSnapshot, () -> CachePolicyEnforcer.enforce(item)));
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
                        Uni<O> result = applyOneToOneWithCache(step, item, cacheReadSupport, contextSnapshot)
                            .onItem().transformToUni(enforced ->
                                withPipelineContext(contextSnapshot, () -> CachePolicyEnforcer.enforce(enforced)));
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
                        Uni<O> result = applyOneToOneWithCache(step, item, cacheReadSupport, contextSnapshot)
                            .onItem().transformToUni(enforced ->
                                withPipelineContext(contextSnapshot, () -> CachePolicyEnforcer.enforce(enforced)));
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

    /**
     * Apply a one-to-one pipeline step while honoring cache-read behavior when available.
     *
     * If no CacheReadSupport is provided or the step opts out of caching, the step is executed
     * directly. Otherwise the method resolves the effective cache policy and key; if a cached
     * value is found it is returned, if the policy requires a cache and a key or cached value
     * cannot be obtained a failed `Uni` is returned, and otherwise the step is executed and its
     * result returned.
     *
     * @param step the one-to-one step to execute
     * @param item the input item for the step
     * @param cacheReadSupport optional cache read helper; when `null` caching is bypassed
     * @param contextSnapshot pipeline execution context used for resolving policy, keys, and versioning
     * @return a `Uni` that yields the cached value when present, the step result when not cached, or a failed `Uni` when the cache policy requires a key or a cached value but none can be resolved
     */
    private static <I, O> Uni<O> applyOneToOneWithCache(
        StepOneToOne<I, O> step,
        I item,
        CacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot) {
        if (cacheReadSupport == null) {
            return withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
        }
        if (step instanceof CacheReadBypass) {
            return withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
        }
        CachePolicy policy = cacheReadSupport.resolvePolicy(contextSnapshot);
        if (!cacheReadSupport.shouldRead(policy)) {
            return withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
        }
        Class<?> targetType = null;
        if (step instanceof CacheKeyTarget cacheKeyTarget) {
            targetType = cacheKeyTarget.cacheKeyTargetType();
        }
        Optional<String> resolvedKey = cacheReadSupport.resolveKey(item, contextSnapshot, targetType);
        if (resolvedKey.isEmpty()) {
            if (policy == CachePolicy.REQUIRE_CACHE) {
                return withPipelineContext(contextSnapshot, () -> Uni.createFrom().failure(
                    new IllegalStateException("Cache key required but could not be resolved")));
            }
            return withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
        }
        String key = cacheReadSupport.withVersionPrefix(resolvedKey.get(), contextSnapshot);
        return cacheReadSupport.reader.get(key)
            .onItem().transformToUni(cached -> {
                if (cached.isPresent()) {
                    PipelineCacheStatusHolder.set(CacheStatus.HIT);
                    @SuppressWarnings("unchecked")
                    O value = (O) cached.get();
                    return Uni.createFrom().item(value);
                }
                if (policy == CachePolicy.REQUIRE_CACHE) {
                    return Uni.createFrom().failure(new CachePolicyViolation(
                        "Cache policy REQUIRE_CACHE failed for key: " + key));
                }
                return withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
            });
    }

    /**
     * Executes the given supplier with the provided PipelineContext set on PipelineContextHolder,
     * restoring the previous context (or clearing it) after execution.
     *
     * @param context  the PipelineContext to set for the duration of the supplier execution; may be null
     * @param supplier the supplier to execute while the context is active
     * @param <T>      the supplier's return type
     * @return the value returned by the supplier
     */
    private static <T> T withPipelineContext(
        PipelineContext context,
        java.util.function.Supplier<T> supplier) {
        if (context == null) {
            return supplier.get();
        }
        PipelineContext previous = PipelineContextHolder.get();
        PipelineContextHolder.set(context);
        try {
            return supplier.get();
        } finally {
            if (previous != null) {
                PipelineContextHolder.set(previous);
            } else {
                PipelineContextHolder.clear();
            }
        }
    }

    static final class CacheReadSupport {
        private final PipelineCacheReader reader;
        private final List<CacheKeyStrategy> strategies;
        private final String defaultPolicy;
        /**
         * Creates a CacheReadSupport that encapsulates a cache reader, ordered key strategies, and a default cache policy.
         *
         * @param reader the PipelineCacheReader used to read cached values
         * @param strategies the list of CacheKeyStrategy instances (ordered by priority) used to resolve cache keys
         * @param defaultPolicy the default cache policy value (configuration string) to use when no policy is present in the PipelineContext
         */
        CacheReadSupport(PipelineCacheReader reader, List<CacheKeyStrategy> strategies, String defaultPolicy) {
            this.reader = reader;
            this.strategies = strategies;
            this.defaultPolicy = defaultPolicy;
        }

        /**
         * Resolve a cache key for the provided item and pipeline context using available cache key strategies.
         *
         * <p>If a non-blank key is found it will be trimmed and returned; if no strategy yields a key or the
         * item is null, an empty Optional is returned.
         *
         * @param item the value to derive a cache key for (may be null)
         * @param context the pipeline context used by strategies to resolve the key
         * @return an Optional containing the resolved, trimmed cache key, or empty if none could be resolved
         */
        Optional<String> resolveKey(Object item, PipelineContext context) {
            return resolveKey(item, context, null);
        }

        /**
         * Resolves a cache key for the given item and context, optionally preferring strategies that support a specific target type.
         *
         * @param item the object to produce a cache key for; when null, resolution is skipped and an empty Optional is returned
         * @param context pipeline execution context used by strategies when resolving a key
         * @param targetType optional preferred target type; when provided, strategies that declare support for this targetType are attempted first
         * @return an Optional containing the first non-blank, trimmed key produced by the configured strategies, or empty if no key can be resolved
         */
        Optional<String> resolveKey(Object item, PipelineContext context, Class<?> targetType) {
            if (item == null) {
                return Optional.empty();
            }
            if (targetType != null) {
                for (CacheKeyStrategy strategy : strategies) {
                    if (!strategy.supportsTarget(targetType)) {
                        continue;
                    }
                    Optional<String> resolved = strategy.resolveKey(item, context);
                    if (resolved.isPresent()) {
                        String key = resolved.get();
                        if (!key.isBlank()) {
                            return Optional.of(key.trim());
                        }
                    }
                }
            }
            for (CacheKeyStrategy strategy : strategies) {
                Optional<String> resolved = strategy.resolveKey(item, context);
                if (resolved.isPresent()) {
                    String key = resolved.get();
                    if (!key.isBlank()) {
                        return Optional.of(key.trim());
                    }
                }
            }
            return Optional.empty();
        }

        /**
         * Determine the cache policy that should apply for the provided pipeline context.
         *
         * @param context the pipeline context whose cache policy should be used; if null or if the context does not specify a policy, the runner's configured default policy is used
         * @return the resolved {@link CachePolicy} corresponding to the context's configured policy or the default policy when none is provided
         */
        CachePolicy resolvePolicy(PipelineContext context) {
            String policy = context != null ? context.cachePolicy() : defaultPolicy;
            return CachePolicy.fromConfig(policy);
        }

        /**
         * Determine whether the given cache policy indicates the cache should be consulted.
         *
         * @param policy the cache policy to evaluate
         * @return `true` if the policy is `RETURN_CACHED` or `REQUIRE_CACHE`, `false` otherwise
         */
        boolean shouldRead(CachePolicy policy) {
            if (policy == null) {
                return false;
            }
            return policy == CachePolicy.RETURN_CACHED || policy == CachePolicy.REQUIRE_CACHE;
        }

        /**
         * Prefixes the provided cache key with the pipeline context's version tag followed by ':' when a non-blank version tag is present.
         *
         * @param key     the cache key to prefix; may be null or empty
         * @param context the pipeline context supplying an optional version tag; may be null
         * @return the original key prefixed with `<version>:` when context contains a non-blank version tag, otherwise the original key
         */
        String withVersionPrefix(String key, PipelineContext context) {
            if (context == null) {
                return key;
            }
            String versionTag = context.versionTag();
            if (versionTag == null || versionTag.isBlank()) {
                return key;
            }
            return versionTag + ":" + key;
        }
    }

    /**
     * Applies a StepOneToOneCompletableFuture to the given reactive input, propagating the pipeline context,
     * optional telemetry instrumentation, and optional per-item parallel execution.
     *
     * @param step the one-to-one step that returns a CompletableFuture-style `Uni` for each input item
     * @param current the current reactive stream, either a `Uni<I>` or a `Multi<I>`
     * @param parallel whether to execute per-item conversions in parallel for `Multi` inputs
     * @param maxConcurrency maximum concurrency for parallel `Multi` processing (merged degree)
     * @param telemetry optional telemetry integration; when non-null item consumption, production and step execution are instrumented
     * @param telemetryContext telemetry run context to associate with instrumentation calls
     * @param contextSnapshot pipeline context snapshot to set while invoking the step
     * @return a transformed reactive stream: a `Uni<O>` when `current` is a `Uni<I>`, or a `Multi<O>` when `current` is a `Multi<I>`
     * @throws IllegalArgumentException if `current` is not a `Uni` or `Multi`
     */
    @SuppressWarnings("unchecked")
    private static <I, O> Object applyOneToOneFutureUnchecked(
        StepOneToOneCompletableFuture<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<I> finalInput = input;
            Uni<O> result = withPipelineContext(contextSnapshot, () -> step.apply(finalInput));
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
                        Uni<O> result = withPipelineContext(contextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)));
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
                        Uni<O> result = withPipelineContext(contextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)));
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

    /**
     * Applies a blocking one-to-many pipeline step to the provided reactive input.
     *
     * <p>If `current` is a `Uni<I>`, the step is applied once and a `Multi<O>` of results is returned.
     * If `current` is a `Multi<I>`, the step is applied for each item and a `Multi<O>` of all results is returned;
     * when `parallel` is true the per-item `Multi<O>` streams are merged with the provided `maxConcurrency`,
     * otherwise they are concatenated in source order. When `telemetry` is non-null, item consumption,
     * production and per-step execution are instrumented. The step is executed with the provided
     * `contextSnapshot` bound for the duration of each invocation.
     *
     * @param step the blocking one-to-many step to execute
     * @param current the current reactive source; must be a `Uni<I>` or `Multi<I>`
     * @param parallel if true, per-item result streams are merged with `maxConcurrency`; otherwise they are concatenated
     * @param maxConcurrency maximum concurrent inner streams when merging
     * @param telemetry optional telemetry component used to instrument consumption, production, and step execution
     * @param telemetryContext telemetry run context passed to instrumentation hooks
     * @param contextSnapshot pipeline context to bind during step execution
     * @return a `Multi<O>` (returned as `Object`): for a `Uni<I>` input a `Multi<O>` of that single invocation's outputs;
     *         for a `Multi<I>` input a `Multi<O>` that merges or concatenates per-item outputs according to `parallel`
     * @throws IllegalArgumentException if `current` is not a `Uni` or `Multi`
     */
    @SuppressWarnings({"unchecked"})
    private static <I, O> Object applyOneToManyBlockingUnchecked(
        StepOneToManyBlocking<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<I> finalInput = input;
            Multi<O> result = withPipelineContext(contextSnapshot, () -> step.apply(finalInput));
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
                        Multi<O> result = withPipelineContext(contextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)));
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
                        Multi<O> result = withPipelineContext(contextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)));
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

    /**
     * Apply a one-to-many pipeline step to the provided reactive input.
     *
     * <p>The method accepts either a Uni or Multi as `current`. For a Uni input it invokes the step
     * to produce a Multi; for a Multi input it invokes the step per item and either merges or
     * concatenates the resulting Multis depending on `parallel`. Telemetry hooks and a pipeline
     * context snapshot are applied when provided.
     *
     * @param step the StepOneToMany to apply
     * @param current the current reactive stream (a Uni or a Multi)
     * @param parallel if true, per-item results are merged with limited concurrency; if false, results are concatenated
     * @param maxConcurrency maximum concurrency for merge when `parallel` is true
     * @param telemetry optional telemetry integration (may be null)
     * @param telemetryContext telemetry run context associated with this execution (may be null)
     * @param contextSnapshot snapshot of the PipelineContext to apply during step invocation (may be null)
     * @return a Uni<O> when `current` is a Uni, or a Multi<O> when `current` is a Multi
     * @throws IllegalArgumentException if `current` is not a Uni or a Multi
     */
    @SuppressWarnings({"unchecked"})
    private static <I, O> Object applyOneToManyUnchecked(
        StepOneToMany<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<I> finalInput = input;
            Multi<O> result = withPipelineContext(contextSnapshot, () -> step.apply(finalInput));
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
                        Multi<O> result = withPipelineContext(contextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)));
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
                        Multi<O> result = withPipelineContext(contextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)));
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

    /**
     * Applies a ManyToOne step to the provided reactive input stream (Uni or Multi) and returns the resulting Uni.
     *
     * @param step the ManyToOne step to execute
     * @param current the current reactive stream; must be either a `Uni<I>` or `Multi<I>`
     * @param telemetry optional telemetry instance used to instrument consumed items, produced items, and the step
     * @param telemetryContext telemetry run context passed to instrumentation calls
     * @param contextSnapshot pipeline context to propagate during step execution
     * @return the step result as a `Uni<O>` (possibly instrumented when `telemetry` is non-null)
     * @throws IllegalArgumentException if `current` is not a `Uni` or `Multi`
     */
    @SuppressWarnings("unchecked")
    private static <I, O> Object applyManyToOneUnchecked(
        ManyToOne<I, O> step,
        Object current,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        if (current instanceof Multi<?>) {
            Multi<I> input = (Multi<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Multi<I> finalInput = input;
            Uni<O> result = withPipelineContext(contextSnapshot, () -> step.apply(finalInput));
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
            Multi<I> finalInput = input.toMulti();
            Uni<O> result = withPipelineContext(contextSnapshot, () -> step.apply(finalInput));
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false);
        } else {
            throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepManyToOne: {0}", current));
        }
    }

    /**
     * Apply a many-to-many pipeline step to the provided reactive source.
     *
     * <p>The method accepts either a Uni (converted to a Multi) or a Multi and returns the step's
     * resulting Multi. If telemetry is provided, item consumption, production and the step are
     * instrumented. The step is executed with the supplied pipeline context snapshot.
     *
     * @param step the many-to-many pipeline step to apply
     * @param current the reactive source to process; must be a Uni or a Multi
     * @param telemetry optional telemetry implementation used to instrument consumption/production and step execution
     * @param telemetryContext telemetry run context passed to instrumentation calls
     * @param contextSnapshot pipeline context to set during step execution
     * @return the resulting Multi produced by the step
     * @throws IllegalArgumentException if {@code current} is not a Uni or Multi
     */
    @SuppressWarnings("unchecked")
    private static <I, O> Object applyManyToManyUnchecked(
        StepManyToMany<I, O> step,
        Object current,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        if (current instanceof Uni<?>) {
            // Single async source — convert to Multi and process
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Multi<I> finalInput = input.toMulti();
            Multi<O> result = withPipelineContext(contextSnapshot, () -> step.apply(finalInput));
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
            Multi<I> finalInput = input;
            Multi<O> result = withPipelineContext(contextSnapshot, () -> step.apply(finalInput));
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