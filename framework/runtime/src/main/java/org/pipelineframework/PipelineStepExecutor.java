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
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.ClientProxy;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.branching.PipelineBranchRoutingException;
import org.pipelineframework.branching.PipelineBranchingRegistry;
import org.pipelineframework.branching.StepBranchingDescriptor;
import org.pipelineframework.cache.CacheKeyTarget;
import org.pipelineframework.cache.CachePolicy;
import org.pipelineframework.cache.CachePolicyEnforcer;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.cache.CacheReadBypass;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.cache.PipelineCacheWriter;
import org.pipelineframework.cache.QueryNotFoundCacheEntry;
import org.pipelineframework.command.CommandStep;
import org.pipelineframework.connector.QueryCacheability;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitStreamOneToOneStep;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.query.ProviderQueryStep;
import org.pipelineframework.query.QueryCacheRequirements;
import org.pipelineframework.query.QueryNotFoundException;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;
import org.pipelineframework.telemetry.PipelineRunContext;
import org.pipelineframework.telemetry.PipelineRunContextHolder;
import org.pipelineframework.telemetry.PipelineRetryTelemetry;
import org.pipelineframework.telemetry.PipelineStepTelemetry;

@ApplicationScoped
class PipelineStepExecutor {

    private static final Logger logger = Logger.getLogger(PipelineStepExecutor.class);
    private static final PipelineInvocationRuntime DEFAULT_INVOCATION_RUNTIME = new PipelineInvocationRuntime();

    @Inject
    PipelineBranchingRegistry branchingRegistry;

    @SuppressWarnings("unchecked")
    Object applyStep(
        Object step,
        Object current,
        org.pipelineframework.config.ParallelismPolicy parallelismPolicy,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        return applyStep(
            step,
            current,
            parallelismPolicy,
            maxConcurrency,
            PipelineStepTelemetry.of(telemetry, telemetryContext),
            cacheReadSupport,
            contextSnapshot,
            awaitContextSnapshot);
    }

    @SuppressWarnings("unchecked")
    Object applyStep(
        Object step,
        Object current,
        org.pipelineframework.config.ParallelismPolicy parallelismPolicy,
        int maxConcurrency,
        PipelineStepTelemetry stepTelemetry,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        Object resolvedStep = unwrapClientProxy(step).orElse(step);
        StepBranchingDescriptor branchingDescriptor = branchingRegistry == null
            ? null
            : branchingRegistry.descriptorFor(resolvedStep.getClass()).orElse(null);
        if (resolvedStep instanceof AwaitStreamOneToOneStep<?, ?> awaitStep && current instanceof Multi<?>) {
            return applyAwaitStreamOneToOneUnchecked(
                awaitStep,
                current,
                null,
                stepTelemetry,
                contextSnapshot,
                awaitContextSnapshot);
        }
        if (resolvedStep instanceof StepOneToOne<?, ?> stepOneToOne) {
            boolean parallel = PipelineParallelismPolicyResolver.shouldParallelize(
                stepOneToOne,
                parallelismPolicy,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE);
            return applyOneToOne(stepOneToOne, current, parallel, maxConcurrency, stepTelemetry, cacheReadSupport,
                contextSnapshot, awaitContextSnapshot, branchingDescriptor);
        } else if (resolvedStep instanceof StepOneToOneCompletableFuture<?, ?> stepFuture) {
            boolean parallel = PipelineParallelismPolicyResolver.shouldParallelize(
                stepFuture,
                parallelismPolicy,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE_FUTURE);
            return applyOneToOneFuture(stepFuture, current, parallel, maxConcurrency, stepTelemetry,
                contextSnapshot, awaitContextSnapshot, branchingDescriptor);
        } else if (resolvedStep instanceof StepOneToMany<?, ?> stepOneToMany) {
            boolean parallel = PipelineParallelismPolicyResolver.shouldParallelize(
                stepOneToMany,
                parallelismPolicy,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_MANY);
            return applyOneToMany(stepOneToMany, current, parallel, maxConcurrency, stepTelemetry,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof ManyToOne<?, ?> manyToOne) {
            return applyManyToOne(manyToOne, current, stepTelemetry, contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof StepManyToMany<?, ?> manyToMany) {
            return applyManyToMany(manyToMany, current, stepTelemetry,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof ReactiveService<?, ?> reactiveService) {
            var adapter = new ReactiveServiceStepAdapter((ReactiveService<Object, Object>) reactiveService);
            return applyOneToOne(adapter, current, false, maxConcurrency, stepTelemetry, cacheReadSupport,
                contextSnapshot, awaitContextSnapshot, branchingDescriptor);
        } else if (resolvedStep instanceof ReactiveStreamingService<?, ?> streamingService) {
            var adapter = new ReactiveStreamingServiceStepAdapter((ReactiveStreamingService<Object, Object>) streamingService);
            return applyOneToMany(adapter, current, false, maxConcurrency, stepTelemetry,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof ReactiveBidirectionalStreamingService<?, ?> bidirectionalStreamingService) {
            var adapter = new ReactiveBidirectionalStreamingServiceStepAdapter(
                (ReactiveBidirectionalStreamingService<Object, Object>) bidirectionalStreamingService);
            return applyManyToMany(adapter, current, stepTelemetry,
                contextSnapshot, awaitContextSnapshot);
        } else {
            String stepType = resolvedStep == null ? "null" : resolvedStep.getClass().getName();
            throw new IllegalArgumentException("Step not recognised: " + stepType);
        }
    }

    private static final class ReactiveServiceStepAdapter extends ConfigurableStep implements StepOneToOne<Object, Object> {
        private final ReactiveService<Object, Object> service;

        private ReactiveServiceStepAdapter(ReactiveService<Object, Object> service) {
            this.service = service;
        }

        @Override
        public Uni<Object> applyOneToOne(Object in) {
            return service.process(in);
        }
    }

    private static final class ReactiveStreamingServiceStepAdapter extends ConfigurableStep implements StepOneToMany<Object, Object> {
        private final ReactiveStreamingService<Object, Object> service;

        private ReactiveStreamingServiceStepAdapter(ReactiveStreamingService<Object, Object> service) {
            this.service = service;
        }

        @Override
        public Multi<Object> applyOneToMany(Object in) {
            return service.process(in);
        }
    }

    private static final class ReactiveBidirectionalStreamingServiceStepAdapter extends ConfigurableStep
        implements StepManyToMany<Object, Object> {
        private final ReactiveBidirectionalStreamingService<Object, Object> service;

        private ReactiveBidirectionalStreamingServiceStepAdapter(ReactiveBidirectionalStreamingService<Object, Object> service) {
            this.service = service;
        }

        @Override
        public Multi<Object> applyTransform(Multi<Object> input) {
            return service.process(input);
        }
    }

    private Optional<Object> unwrapClientProxy(Object step) {
        if (step == null) {
            return Optional.empty();
        }
        if (!(step instanceof ClientProxy clientProxy)) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(clientProxy.arc_contextualInstance());
        } catch (RuntimeException e) {
            logger.debugf(e, "Failed to unwrap pipeline step client proxy %s", step.getClass().getName());
            return Optional.empty();
        }
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToOneUnchecked(StepOneToOne<I, O> step, Object current) {
        return applyOneToOneUnchecked(
            step,
            current,
            false,
            PipelineParallelismPolicyResolver.DEFAULT_MAX_CONCURRENCY,
            null,
            null,
            null,
            null);
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToOneUnchecked(
        StepOneToOne<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        return applyOneToOneUnchecked(
            step,
            current,
            parallel,
            maxConcurrency,
            null,
            null,
            cacheReadSupport,
            contextSnapshot,
            awaitContextSnapshot);
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToOneUnchecked(
        StepOneToOne<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot) {
        return applyOneToOneUnchecked(
            step,
            current,
            parallel,
            maxConcurrency,
            telemetry,
            telemetryContext,
            cacheReadSupport,
            contextSnapshot,
            null);
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToOneUnchecked(
        StepOneToOne<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        return applyOneToOneUnchecked(
            step,
            current,
            parallel,
            maxConcurrency,
            telemetry,
            telemetryContext,
            cacheReadSupport,
            contextSnapshot,
            awaitContextSnapshot,
            null);
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToOneUnchecked(
        StepOneToOne<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot,
        StepBranchingDescriptor branchingDescriptor) {
        return applyOneToOne(
            step,
            current,
            parallel,
            maxConcurrency,
            PipelineStepTelemetry.of(telemetry, telemetryContext),
            cacheReadSupport,
            contextSnapshot,
            awaitContextSnapshot,
            branchingDescriptor);
    }

    @SuppressWarnings({"unchecked"})
    private static <I, O> Object applyOneToOne(
        StepOneToOne<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry telemetry,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot,
        StepBranchingDescriptor branchingDescriptor) {
        if (current instanceof Uni<?>) {
            Uni<I> input = telemetry.consume(step.getClass(), (Uni<I>) current);
            Uni<O> result = input
                .onItem()
                .transformToUni(item -> {
                    I applicable = applicableInput(branchingDescriptor, item);
                    if (applicable == null) {
                        return skippedUni(step.getClass(), item, branchingDescriptor, telemetry);
                    }
                    var replayScope = telemetry.beginReplayStep(step.getClass(), false, applicable);
                    Uni<O> scoped = applyOneToOneWithCache(
                        step,
                        applicable,
                        cacheReadSupport,
                        contextSnapshot,
                        awaitContextSnapshot,
                        telemetry,
                        replayScope)
                        .onItem().transformToUni(enforced -> applyCachePolicy(step, enforced, contextSnapshot))
                        .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
                    return telemetry.instrument(step.getClass(), scoped, false, replayScope);
                })
                ;
            return telemetry.produce(step.getClass(), result);
        } else if (current instanceof Multi<?>) {
            Multi<I> multi = telemetry.consume(step.getClass(), (Multi<I>) current);
            if (parallel) {
                logger.debugf("Applying step %s (merge)", step.getClass());
                return multi
                    .onItem()
                    .transformToUni(item -> {
                    I applicable = applicableInput(branchingDescriptor, item);
                    if (applicable == null) {
                        return skippedUni(step.getClass(), item, branchingDescriptor, telemetry);
                    }
                    var replayScope = telemetry.beginReplayStep(step.getClass(), true, applicable);
                        Uni<O> result = applyOneToOneWithCache(
                            step,
                            applicable,
                            cacheReadSupport,
                            contextSnapshot,
                            awaitContextSnapshot,
                            telemetry,
                            replayScope)
                            .onItem().transformToUni(enforced ->
                                applyCachePolicy(step, enforced, contextSnapshot))
                            .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
                        return telemetry.instrument(
                            step.getClass(), telemetry.produce(step.getClass(), result), true, replayScope);
                    })
                    .merge(maxConcurrency);
            }
            logger.debugf("Applying step %s (concatenate)", step.getClass());
            return multi
                .onItem()
                .transformToUni(item -> {
                    I applicable = applicableInput(branchingDescriptor, item);
                    if (applicable == null) {
                        return skippedUni(step.getClass(), item, branchingDescriptor, telemetry);
                    }
                    var replayScope = telemetry.beginReplayStep(step.getClass(), true, applicable);
                    Uni<O> result = applyOneToOneWithCache(
                        step,
                        applicable,
                        cacheReadSupport,
                        contextSnapshot,
                        awaitContextSnapshot,
                        telemetry,
                        replayScope)
                        .onItem().transformToUni(enforced ->
                            applyCachePolicy(step, enforced, contextSnapshot))
                        .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
                    return telemetry.instrument(
                        step.getClass(), telemetry.produce(step.getClass(), result), true, replayScope);
                })
                .concatenate();
        }
        throw new IllegalArgumentException(MessageFormat.format(
            "Unsupported current type for StepOneToOne: {0}",
            current == null ? "null" : current.getClass().getName()));
    }

    @SuppressWarnings("unchecked")
    private static <I, O> Multi<O> applyAwaitStreamOneToOneUnchecked(
        AwaitStreamOneToOneStep<I, O> step,
        Object current,
        StepBranchingDescriptor branchingDescriptor,
        PipelineStepTelemetry telemetry,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        Multi<I> input = telemetry.consume(step.getClass(), (Multi<I>) current);
        Multi<I> finalInput = scopedMultiInput(input, contextSnapshot, awaitContextSnapshot);
        if (branchingDescriptor != null) {
            Multi<O> result = finalInput
                .onItem()
                .transformToMulti(item -> {
                    I applicable = applicableInput(branchingDescriptor, item);
                    if (applicable == null) {
                        return skippedMulti(step.getClass(), item, branchingDescriptor, telemetry);
                    }
                    var replayScope = telemetry.beginReplayStep(step.getClass(), true, applicable);
                    Multi<O> scoped = withStepExecutionMulti(
                        contextSnapshot,
                        awaitContextSnapshot,
                        () -> step.applyAwaitPerItem(Multi.createFrom().item(applicable)))
                        .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
                    return telemetry.instrument(step.getClass(), scoped, true, replayScope);
                })
                .concatenate();
            return telemetry.produce(step.getClass(), result);
        }
        Multi<O> result = withStepExecutionMulti(
            contextSnapshot,
            awaitContextSnapshot,
            () -> step.applyAwaitPerItem(finalInput));
        return telemetry.produce(step.getClass(), result);
    }

    private static <O> Uni<O> applyCachePolicy(
        StepOneToOne<?, O> step,
        O item,
        PipelineContext contextSnapshot) {
        if (step instanceof CacheReadBypass) {
            PipelineCacheStatusHolder.clear();
            return Uni.createFrom().item(item);
        }
        return withPipelineContext(contextSnapshot, () -> CachePolicyEnforcer.enforce(item));
    }

    private static <I, O> Uni<O> applyOneToOneWithCache(
        StepOneToOne<I, O> step,
        I item,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot,
        PipelineStepTelemetry telemetry,
        PipelineStepTelemetry.ReplayScope replayScope) {
        CachePolicy policy = cacheReadSupport == null
            ? contextCachePolicy(contextSnapshot)
            : cacheReadSupport.resolvePolicy(contextSnapshot);
        if (step instanceof CommandStep && policy == CachePolicy.SKIP_IF_PRESENT) {
            return withPipelineContext(contextSnapshot, () -> Uni.createFrom().failure(
                new CachePolicyViolation(
                    "Cache policy SKIP_IF_PRESENT is not supported for Command steps because it can execute "
                        + "a live external effect while retaining an older pipeline replay output; use "
                        + "PREFER_CACHE, REQUIRE_CACHE, CACHE_ONLY, or BYPASS_CACHE")));
        }
        if (cacheReadSupport == null) {
            return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        if (step instanceof ProviderQueryStep queryStep) {
            try {
                validateQueryCachePolicy(queryStep.queryCacheRequirements(), policy, cacheReadSupport);
            } catch (CachePolicyViolation failure) {
                return withPipelineContext(contextSnapshot, () -> Uni.createFrom().failure(failure));
            }
        }
        if (step instanceof CacheReadBypass) {
            return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        if (!cacheReadSupport.shouldRead(policy)) {
            if (step instanceof ProviderQueryStep queryStep
                && policy == CachePolicy.CACHE_ONLY
                && queryStep.queryCacheRequirements().negativeCacheTtl().isPresent()) {
                Optional<String> key = resolveCacheKey(step, item, cacheReadSupport, contextSnapshot);
                if (key.isEmpty()) {
                    return withPipelineContext(contextSnapshot, () -> Uni.createFrom().failure(
                        new CachePolicyViolation(
                            "negative caching requires a cache key for Query operation "
                                + queryStep.queryCacheRequirements().operationIdentity())));
                }
                return withPipelineContext(contextSnapshot, () -> {
                    PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                    return executeOneToOne(
                        step, item, contextSnapshot, awaitContextSnapshot, cacheReadSupport, policy, key);
                });
            }
            return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        java.util.Optional<String> resolvedKey = resolveCacheKey(step, item, cacheReadSupport, contextSnapshot);
        if (resolvedKey.isEmpty()) {
            if (policy == CachePolicy.REQUIRE_CACHE) {
                return withPipelineContext(contextSnapshot, () -> Uni.createFrom().failure(
                    new IllegalStateException("Cache key required but could not be resolved")));
            }
            if (step instanceof ProviderQueryStep queryStep
                && queryStep.queryCacheRequirements().negativeCacheTtl().isPresent()) {
                return withPipelineContext(contextSnapshot, () -> Uni.createFrom().failure(
                    new CachePolicyViolation(
                        "negative caching requires a cache key for Query operation "
                            + queryStep.queryCacheRequirements().operationIdentity())));
            }
            return withPipelineContext(contextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.MISS);
                return executeOneToOne(
                    step, item, contextSnapshot, awaitContextSnapshot, cacheReadSupport, policy, Optional.empty());
            });
        }
        String key = resolvedKey.orElseThrow();
        return cacheReadSupport.reader().get(key)
            .onItemOrFailure().transformToUni((cached, failure) -> {
                if (failure != null) {
                    if (policy == CachePolicy.REQUIRE_CACHE) {
                        return Uni.createFrom().failure(failure);
                    }
                    return withPipelineContext(contextSnapshot, () -> {
                        PipelineCacheStatusHolder.set(CacheStatus.MISS);
                        return executeOneToOne(
                            step, item, contextSnapshot, awaitContextSnapshot, cacheReadSupport, policy,
                            Optional.of(key));
                    });
                }
                if (cached.isPresent()) {
                    return withPipelineContext(contextSnapshot, () -> {
                        PipelineCacheStatusHolder.set(CacheStatus.HIT);
                        telemetry.recordCacheHit(replayScope);
                        try {
                            Object cachedValue = cached.orElseThrow();
                            if (cachedValue instanceof QueryNotFoundCacheEntry notFound) {
                                if (step instanceof ProviderQueryStep) {
                                    return Uni.createFrom().<O>failure(
                                        new QueryNotFoundException(notFound.outcomeCode()));
                                }
                                return Uni.createFrom().failure(new IllegalStateException(
                                    "Cached Query NotFound marker for key '" + key
                                        + "' cannot be returned as ordinary pipeline output for step "
                                        + step.getClass().getName()));
                            }
                            @SuppressWarnings("unchecked")
                            O value = (O) cachedValue;
                            return Uni.createFrom().item(value);
                        } catch (ClassCastException ex) {
                            return Uni.createFrom().failure(new IllegalStateException(
                                "Cached value for key '" + key + "' is incompatible with step "
                                    + step.getClass().getName(), ex));
                        }
                    });
                }
                if (policy == CachePolicy.REQUIRE_CACHE) {
                    return Uni.createFrom().failure(new CachePolicyViolation(
                        "Cache policy REQUIRE_CACHE failed for key: " + key));
                }
                return withPipelineContext(contextSnapshot, () -> {
                    PipelineCacheStatusHolder.set(CacheStatus.MISS);
                    return executeOneToOne(
                        step, item, contextSnapshot, awaitContextSnapshot, cacheReadSupport, policy,
                        Optional.of(key));
                });
            });
    }

    private static <I, O> Uni<O> executeOneToOne(
        StepOneToOne<I, O> step,
        I item,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot,
        PipelineCacheReadSupport cacheReadSupport,
        CachePolicy policy,
        Optional<String> cacheKey
    ) {
        Uni<O> execution = withStepExecutionUni(
            contextSnapshot, awaitContextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
        if (!(step instanceof ProviderQueryStep queryStep)
            || policy == CachePolicy.BYPASS_CACHE
            || policy == CachePolicy.REQUIRE_CACHE
            || queryStep.queryCacheRequirements().negativeCacheTtl().isEmpty()) {
            return execution;
        }
        if (cacheKey.isEmpty()) {
            return execution;
        }
        QueryCacheRequirements requirements = queryStep.queryCacheRequirements();
        return execution.onFailure(QueryNotFoundException.class).call(failure -> {
            QueryNotFoundException notFound = (QueryNotFoundException) failure;
            PipelineCacheWriter writer = cacheReadSupport.writer().orElseThrow(() -> new CachePolicyViolation(
                "negative caching for Query operation " + requirements.operationIdentity()
                    + " requires a cache subsystem that supports bounded writes"));
            return writer.put(
                cacheKey.orElseThrow(),
                new QueryNotFoundCacheEntry(notFound.outcomeCode()),
                requirements.negativeCacheTtl().orElseThrow());
        });
    }

    private static Optional<String> resolveCacheKey(
        StepOneToOne<?, ?> step,
        Object item,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot
    ) {
        Class<?> targetType = step instanceof CacheKeyTarget cacheKeyTarget
            ? cacheKeyTarget.cacheKeyTargetType()
            : null;
        return cacheReadSupport.resolveKey(item, contextSnapshot, targetType)
            .map(key -> cacheReadSupport.withVersionPrefix(key, contextSnapshot));
    }

    private static void validateQueryCachePolicy(
        QueryCacheRequirements requirements,
        CachePolicy policy,
        PipelineCacheReadSupport cacheReadSupport
    ) {
        if (policy == CachePolicy.SKIP_IF_PRESENT) {
            throw new CachePolicyViolation(
                "Cache policy SKIP_IF_PRESENT is not supported for provider-backed Query steps because it performs "
                    + "a live observation but neither replays the existing value nor records a miss; use "
                    + "PREFER_CACHE, REQUIRE_CACHE, CACHE_ONLY, or BYPASS_CACHE for operation "
                    + requirements.operationIdentity());
        }
        if (requirements.capabilities().cacheability() == QueryCacheability.LIVE_ONLY
            && policy != CachePolicy.BYPASS_CACHE) {
            throw new CachePolicyViolation(
                "Query operation " + requirements.operationIdentity() + " is LIVE_ONLY and requires BYPASS_CACHE, not "
                    + policy);
        }
        if (policy == CachePolicy.BYPASS_CACHE) {
            return;
        }
        requirements.capabilities().maximumCacheAge().ifPresent(maximum -> {
            Optional<Duration> configured = cacheReadSupport == null
                ? Optional.empty()
                : cacheReadSupport.configuredTtl();
            if (configured.isEmpty()) {
                throw new CachePolicyViolation(
                    "Query operation " + requirements.operationIdentity() + " limits positive cache age to "
                        + maximum + " but pipeline.cache.ttl is not configured");
            }
            if (configured.orElseThrow().compareTo(maximum) > 0) {
                throw new CachePolicyViolation(
                    "pipeline.cache.ttl " + configured.orElseThrow() + " exceeds Query operation "
                        + requirements.operationIdentity() + " maximum cache age " + maximum);
            }
        });
        if (requirements.negativeCacheTtl().isPresent()
            && (policy == CachePolicy.RETURN_CACHED || policy == CachePolicy.CACHE_ONLY)
            && (cacheReadSupport == null || cacheReadSupport.writer().isEmpty())) {
            throw new CachePolicyViolation(
                "negative caching for Query operation " + requirements.operationIdentity()
                    + " requires a cache subsystem that supports bounded writes");
        }
    }

    private static CachePolicy contextCachePolicy(PipelineContext context) {
        return context == null || context.cachePolicy() == null
            ? CachePolicy.RETURN_CACHED
            : CachePolicy.fromConfig(context.cachePolicy());
    }

    static <T> T withPipelineContext(PipelineContext context, Supplier<T> supplier) {
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

    static <T> Uni<T> withStepExecutionUni(
        PipelineContext context,
        AwaitExecutionContext awaitContext,
        Supplier<Uni<T>> supplier
    ) {
        return DEFAULT_INVOCATION_RUNTIME.invokeStepUni(context, awaitContext, supplier);
    }

    static <T> Multi<T> withStepExecutionMulti(
        PipelineContext context,
        AwaitExecutionContext awaitContext,
        Supplier<Multi<T>> supplier
    ) {
        return DEFAULT_INVOCATION_RUNTIME.invokeStepMulti(context, awaitContext, supplier);
    }

    static Object contextualizeInput(
        Object input,
        PipelineContext context,
        AwaitExecutionContext awaitContext,
        PipelineRunContext runContext
    ) {
        return PipelineRunContextHolder.call(runContext, () -> {
            if (input instanceof Uni<?> uni) {
                return withStepExecutionUni(context, awaitContext, () -> uni);
            }
            if (input instanceof Multi<?> multi) {
                return withStepExecutionMulti(context, awaitContext, () -> multi);
            }
            throw new IllegalArgumentException("Pipeline input must be a Uni or Multi");
        });
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyOneToOneFutureUnchecked(
        StepOneToOneCompletableFuture<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot) {
        return applyOneToOneFutureUnchecked(
            step,
            current,
            parallel,
            maxConcurrency,
            telemetry,
            telemetryContext,
            contextSnapshot,
            null);
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyOneToOneFutureUnchecked(
        StepOneToOneCompletableFuture<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        return applyOneToOneFutureUnchecked(
            step,
            current,
            parallel,
            maxConcurrency,
            telemetry,
            telemetryContext,
            contextSnapshot,
            awaitContextSnapshot,
            null);
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyOneToOneFutureUnchecked(
        StepOneToOneCompletableFuture<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot,
        StepBranchingDescriptor branchingDescriptor) {
        return applyOneToOneFuture(
            step,
            current,
            parallel,
            maxConcurrency,
            PipelineStepTelemetry.of(telemetry, telemetryContext),
            contextSnapshot,
            awaitContextSnapshot,
            branchingDescriptor);
    }

    @SuppressWarnings("unchecked")
    private static <I, O> Object applyOneToOneFuture(
        StepOneToOneCompletableFuture<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry telemetry,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot,
        StepBranchingDescriptor branchingDescriptor) {
        if (current instanceof Uni<?>) {
            Uni<I> input = telemetry.consume(step.getClass(), (Uni<I>) current);
            Uni<O> result = input.onItem().transformToUni(item ->
                applicableInput(branchingDescriptor, item) != null
                    ? executeFutureUnary(
                        step,
                        applicableInput(branchingDescriptor, item),
                        telemetry,
                        contextSnapshot,
                        awaitContextSnapshot,
                        false)
                    : skippedUni(step.getClass(), item, branchingDescriptor, telemetry));
            return telemetry.produce(step.getClass(), result);
        } else if (current instanceof Multi<?>) {
            Multi<I> multi = telemetry.consume(step.getClass(), (Multi<I>) current);
            if (parallel) {
                return multi
                    .onItem()
                    .transformToUni(item -> {
                        I applicable = applicableInput(branchingDescriptor, item);
                        return applicable != null
                            ? executeFutureUnary(step, applicable, telemetry, contextSnapshot, awaitContextSnapshot, true)
                            : skippedUni(step.getClass(), item, branchingDescriptor, telemetry);
                    })
                    .merge(maxConcurrency);
            }
            return multi
                .onItem()
                .transformToUni(item -> {
                    I applicable = applicableInput(branchingDescriptor, item);
                    return applicable != null
                        ? executeFutureUnary(step, applicable, telemetry, contextSnapshot, awaitContextSnapshot, true)
                        : skippedUni(step.getClass(), item, branchingDescriptor, telemetry);
                })
                .concatenate();
        }
        throw new IllegalArgumentException(MessageFormat.format(
            "Unsupported current type for StepOneToOneCompletableFuture: {0}",
            current == null ? "null" : current.getClass().getName()));
    }

    private static <I, O> Uni<O> executeFutureUnary(
        StepOneToOneCompletableFuture<I, O> step,
        I item,
        PipelineStepTelemetry telemetry,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot,
        boolean perItemOperation) {
        var replayScope = telemetry.beginReplayStep(step.getClass(), perItemOperation, item);
        Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot,
            () -> step.apply(Uni.createFrom().item(item)))
            .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
        return telemetry.instrument(step.getClass(), result, perItemOperation, replayScope);
    }

    private static boolean accepts(StepBranchingDescriptor branchingDescriptor, Object item) {
        return branchingDescriptor == null || branchingDescriptor.accepts(item);
    }

    @SuppressWarnings("unchecked")
    private static <I> I applicableInput(StepBranchingDescriptor branchingDescriptor, Object item) {
        if (branchingDescriptor == null) {
            return (I) item;
        }
        return (I) branchingDescriptor.applicableItem(item);
    }

    @SuppressWarnings("unchecked")
    private static <O> Uni<O> skippedUni(
        Class<?> stepClass,
        Object item,
        StepBranchingDescriptor branchingDescriptor,
        PipelineStepTelemetry telemetry) {
        if (branchingDescriptor == null) {
            return (Uni<O>) (Uni<?>) Uni.createFrom().item(item);
        }
        if (branchingDescriptor.terminal()) {
            return Uni.createFrom().failure(terminalMismatch(branchingDescriptor, item));
        }
        if (item != null && "org.pipelineframework.csv.grpc.PipelineTypes$PaymentStatus".equals(item.getClass().getName())) {
            logger.infof(
                "Branch-aware skip for step %s on PaymentStatus payload: %s",
                branchingDescriptor.stepName(),
                item);
        }
        telemetry.recordSkip(stepClass, item, branchingDescriptor.acceptedContracts(),
            branchingDescriptor.variantIdentity(item));
        return (Uni<O>) (Uni<?>) Uni.createFrom().item(item);
    }

    @SuppressWarnings("unchecked")
    private static <O> Multi<O> skippedMulti(
        Class<?> stepClass,
        Object item,
        StepBranchingDescriptor branchingDescriptor,
        PipelineStepTelemetry telemetry) {
        if (branchingDescriptor == null) {
            return (Multi<O>) (Multi<?>) Multi.createFrom().item(item);
        }
        if (branchingDescriptor.terminal()) {
            return Multi.createFrom().failure(terminalMismatch(branchingDescriptor, item));
        }
        if (item != null && "org.pipelineframework.csv.grpc.PipelineTypes$PaymentStatus".equals(item.getClass().getName())) {
            logger.infof(
                "Branch-aware skip for step %s on PaymentStatus payload: %s",
                branchingDescriptor.stepName(),
                item);
        }
        telemetry.recordSkip(stepClass, item, branchingDescriptor.acceptedContracts(),
            branchingDescriptor.variantIdentity(item));
        return (Multi<O>) (Multi<?>) Multi.createFrom().item(item);
    }

    private static PipelineBranchRoutingException terminalMismatch(
        StepBranchingDescriptor branchingDescriptor,
        Object item) {
        String currentType = item == null ? "null" : item.getClass().getName();
        return new PipelineBranchRoutingException(
            "Branch-aware terminal step '" + branchingDescriptor.stepName()
                + "' cannot accept runtime item type '" + currentType
                + "'. Accepted contracts: " + branchingDescriptor.acceptedContracts());
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToManyUnchecked(
        StepOneToMany<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot) {
        return applyOneToManyUnchecked(
            step,
            current,
            parallel,
            maxConcurrency,
            telemetry,
            telemetryContext,
            contextSnapshot,
            null);
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToManyUnchecked(
        StepOneToMany<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        return applyOneToMany(
            step,
            current,
            parallel,
            maxConcurrency,
            PipelineStepTelemetry.of(telemetry, telemetryContext),
            contextSnapshot,
            awaitContextSnapshot);
    }

    @SuppressWarnings({"unchecked"})
    private static <I, O> Object applyOneToMany(
        StepOneToMany<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineStepTelemetry telemetry,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = telemetry.consume(step.getClass(), (Uni<I>) current);
            var replayScope = telemetry.beginPendingReplayStep(step.getClass(), false);
            Uni<I> finalInput = telemetry.recordInput(replayScope, input);
            Uni<I> replayInput = scopedUniInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Multi<O> result = withStepExecutionMulti(
                contextSnapshot,
                awaitContextSnapshot,
                () -> awaitContextSnapshot == null
                    ? step.apply(replayInput)
                    : applyOneToManyForAwaitParent(step, replayInput))
                .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
            return telemetry.instrument(
                step.getClass(), telemetry.produce(step.getClass(), result), false, replayScope);
        } else if (current instanceof Multi<?>) {
            Multi<I> multi = telemetry.consume(step.getClass(), (Multi<I>) current);
            if (parallel) {
                logger.debugf("Applying step %s (merge)", step.getClass());
                return multi
                    .onItem()
                    .transformToMulti(item -> {
                        var replayScope = telemetry.beginReplayStep(step.getClass(), true, item);
                        Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)))
                            .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
                        return telemetry.instrument(
                            step.getClass(), telemetry.produce(step.getClass(), result), true, replayScope);
                    })
                    .merge(maxConcurrency);
            }
            logger.debugf("Applying step %s (concatenate)", step.getClass());
            return multi
                .onItem()
                .transformToMulti(item -> {
                    var replayScope = telemetry.beginReplayStep(step.getClass(), true, item);
                    Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot,
                        () -> step.apply(Uni.createFrom().item(item)))
                        .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
                    return telemetry.instrument(
                        step.getClass(), telemetry.produce(step.getClass(), result), true, replayScope);
                })
                .concatenate();
        }
        throw new IllegalArgumentException(MessageFormat.format(
            "Unsupported current type for StepOneToMany: {0}",
            current == null ? "null" : current.getClass().getName()));
    }

    private static <I, O> Multi<O> applyOneToManyForAwaitParent(StepOneToMany<I, O> step, Uni<I> input) {
        return input.onItem().transformToMulti(item -> step.applyOneToMany(item)
            .onItem().transform(output -> {
                if (logger.isDebugEnabled()) {
                    logger.debugf(
                        "Step %s emitted item: %s",
                        step.getClass().getSimpleName(),
                        output);
                }
                return output;
            })
            .onFailure(step::shouldRetry)
            .invoke(t -> PipelineRetryTelemetry.recordRetry(step.getClass(), t))
            .onFailure(step::shouldRetry)
            .retry()
            .withBackOff(step.retryWait(), step.maxBackoff())
            .withJitter(step.jitter() ? 0.5 : 0.0)
            .atMost(step.retryLimit())
            .onFailure().recoverWithMulti(t -> {
                if (step.shouldPropagateWithoutRecovery(t)) {
                    return Multi.createFrom().failure(t);
                }
                logger.infof(
                    "Step %s completed all retries (%s attempts) with failure: %s",
                    step.getClass().getSimpleName(),
                    step.retryLimit(),
                    t.getMessage()
                );
                if (step.recoverOnFailure()) {
                    List<I> sample = item == null ? List.of() : List.of(item);
                    return step.rejectStream(sample, item == null ? 0L : 1L, t)
                        .onItem().transformToMulti(ignored -> Multi.createFrom().empty());
                }
                return Multi.createFrom().failure(t);
            }));
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToOneUnchecked(
        ManyToOne<I, O> step,
        Object current,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot) {
        return applyManyToOneUnchecked(step, current, telemetry, telemetryContext, contextSnapshot, null);
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToOneUnchecked(
        ManyToOne<I, O> step,
        Object current,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        return applyManyToOne(
            step,
            current,
            PipelineStepTelemetry.of(telemetry, telemetryContext),
            contextSnapshot,
            awaitContextSnapshot);
    }

    @SuppressWarnings("unchecked")
    private static <I, O> Object applyManyToOne(
        ManyToOne<I, O> step,
        Object current,
        PipelineStepTelemetry telemetry,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Multi<?>) {
            Multi<I> input = telemetry.consume(step.getClass(), (Multi<I>) current);
            var replayScope = telemetry.beginPendingReplayStep(step.getClass(), false);
            Multi<I> finalInput = telemetry.recordInput(replayScope, input);
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
            return telemetry.instrument(
                step.getClass(), telemetry.produce(step.getClass(), result), false, replayScope);
        } else if (current instanceof Uni<?>) {
            Uni<I> input = telemetry.consume(step.getClass(), (Uni<I>) current);
            var replayScope = telemetry.beginPendingReplayStep(step.getClass(), false);
            Multi<I> finalInput = telemetry.recordInput(replayScope, input).toMulti();
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
            return telemetry.instrument(
                step.getClass(), telemetry.produce(step.getClass(), result), false, replayScope);
        }
        throw new IllegalArgumentException(MessageFormat.format(
            "Unsupported current type for StepManyToOne: type={0} value={1}",
            current == null ? "null" : current.getClass().getName(),
            current));
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToManyUnchecked(
        StepManyToMany<I, O> step,
        Object current,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot) {
        return applyManyToManyUnchecked(step, current, telemetry, telemetryContext, contextSnapshot, null);
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToManyUnchecked(
        StepManyToMany<I, O> step,
        Object current,
        PipelineStepTelemetry.Seam telemetry,
        PipelineRunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        return applyManyToMany(
            step,
            current,
            PipelineStepTelemetry.of(telemetry, telemetryContext),
            contextSnapshot,
            awaitContextSnapshot);
    }

    @SuppressWarnings("unchecked")
    private static <I, O> Object applyManyToMany(
        StepManyToMany<I, O> step,
        Object current,
        PipelineStepTelemetry telemetry,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = telemetry.consume(step.getClass(), (Uni<I>) current);
            var replayScope = telemetry.beginPendingReplayStep(step.getClass(), false);
            Multi<I> finalInput = telemetry.recordInput(replayScope, input).toMulti();
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
            return telemetry.instrument(
                step.getClass(), telemetry.produce(step.getClass(), result), false, replayScope);
        } else if (current instanceof Multi<?> input) {
            logger.debugf("Applying many-to-many step %s on full stream", step.getClass());
            Multi<I> typedInput = telemetry.consume(step.getClass(), (Multi<I>) input);
            var replayScope = telemetry.beginPendingReplayStep(step.getClass(), false);
            Multi<I> finalInput = telemetry.recordInput(replayScope, typedInput);
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> telemetry.recordOutput(replayScope, output));
            return telemetry.instrument(
                step.getClass(), telemetry.produce(step.getClass(), result), false, replayScope);
        }
        throw new IllegalArgumentException(MessageFormat.format(
            "Unsupported current type for StepManyToMany: type={0} value={1}",
            current == null ? "null" : current.getClass().getName(),
            current));
    }

    private static <T> Uni<T> scopedUniInput(
        Uni<T> input,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (contextSnapshot == null && awaitContextSnapshot == null) {
            return input;
        }
        return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> input);
    }

    private static <T> Multi<T> scopedMultiInput(
        Multi<T> input,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (contextSnapshot == null && awaitContextSnapshot == null) {
            return input;
        }
        return withStepExecutionMulti(contextSnapshot, awaitContextSnapshot, () -> input);
    }
}
