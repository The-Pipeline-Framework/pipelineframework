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

import io.quarkus.arc.ClientProxy;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.CacheKeyTarget;
import org.pipelineframework.cache.CachePolicy;
import org.pipelineframework.cache.CachePolicyEnforcer;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.cache.CacheReadBypass;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitStreamOneToOneStep;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;
import org.pipelineframework.telemetry.PipelineTelemetry;

@ApplicationScoped
class PipelineStepExecutor {

    private static final Logger logger = Logger.getLogger(PipelineStepExecutor.class);
    private static final PipelineInvocationRuntime DEFAULT_INVOCATION_RUNTIME = new PipelineInvocationRuntime();

    @SuppressWarnings("unchecked")
    Object applyStep(
        Object step,
        Object current,
        org.pipelineframework.config.ParallelismPolicy parallelismPolicy,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        Object resolvedStep = unwrapClientProxy(step).orElse(step);
        if (resolvedStep instanceof AwaitStreamOneToOneStep<?, ?> awaitStep && current instanceof Multi<?>) {
            return applyAwaitStreamOneToOneUnchecked(
                awaitStep,
                current,
                telemetry,
                telemetryContext,
                contextSnapshot,
                awaitContextSnapshot);
        }
        if (resolvedStep instanceof StepOneToOne<?, ?> stepOneToOne) {
            boolean parallel = PipelineParallelismPolicyResolver.shouldParallelize(
                stepOneToOne,
                parallelismPolicy,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE);
            return applyOneToOneUnchecked(stepOneToOne, current, parallel, maxConcurrency, telemetry, telemetryContext, cacheReadSupport,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof StepOneToOneCompletableFuture<?, ?> stepFuture) {
            boolean parallel = PipelineParallelismPolicyResolver.shouldParallelize(
                stepFuture,
                parallelismPolicy,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE_FUTURE);
            return applyOneToOneFutureUnchecked(stepFuture, current, parallel, maxConcurrency, telemetry, telemetryContext,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof StepOneToMany<?, ?> stepOneToMany) {
            boolean parallel = PipelineParallelismPolicyResolver.shouldParallelize(
                stepOneToMany,
                parallelismPolicy,
                PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_MANY);
            return applyOneToManyUnchecked(stepOneToMany, current, parallel, maxConcurrency, telemetry, telemetryContext,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof ManyToOne<?, ?> manyToOne) {
            return applyManyToOneUnchecked(manyToOne, current, telemetry, telemetryContext, contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof StepManyToMany<?, ?> manyToMany) {
            return applyManyToManyUnchecked(manyToMany, current, telemetry, telemetryContext,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof ReactiveService<?, ?> reactiveService) {
            var adapter = new ReactiveServiceStepAdapter((ReactiveService<Object, Object>) reactiveService);
            return applyOneToOneUnchecked(adapter, current, false, maxConcurrency, telemetry, telemetryContext, cacheReadSupport,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof ReactiveStreamingService<?, ?> streamingService) {
            var adapter = new ReactiveStreamingServiceStepAdapter((ReactiveStreamingService<Object, Object>) streamingService);
            return applyOneToManyUnchecked(adapter, current, false, maxConcurrency, telemetry, telemetryContext,
                contextSnapshot, awaitContextSnapshot);
        } else if (resolvedStep instanceof ReactiveBidirectionalStreamingService<?, ?> bidirectionalStreamingService) {
            var adapter = new ReactiveBidirectionalStreamingServiceStepAdapter(
                (ReactiveBidirectionalStreamingService<Object, Object>) bidirectionalStreamingService);
            return applyManyToManyUnchecked(adapter, current, telemetry, telemetryContext,
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<O> result = input
                .onItem()
                .transformToUni(item -> {
                    var replayScope = telemetry == null
                        ? null
                        : telemetry.beginReplayStep(step.getClass(), telemetryContext, false, item);
                    Uni<O> scoped = applyOneToOneWithCache(
                        step,
                        item,
                        cacheReadSupport,
                        contextSnapshot,
                        awaitContextSnapshot,
                        telemetry,
                        replayScope)
                        .onItem().transformToUni(enforced -> applyCachePolicy(step, enforced, contextSnapshot))
                        .onItem().invoke(output -> {
                            if (telemetry != null) {
                                telemetry.recordReplayOutput(replayScope, output);
                            }
                        });
                    return telemetry == null
                        ? scoped
                        : telemetry.instrumentStepUni(step.getClass(), scoped, telemetryContext, false, replayScope);
                })
                ;
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return result;
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
                    var replayScope = telemetry == null
                        ? null
                        : telemetry.beginReplayStep(step.getClass(), telemetryContext, true, item);
                        Uni<O> result = applyOneToOneWithCache(
                            step,
                            item,
                            cacheReadSupport,
                            contextSnapshot,
                            awaitContextSnapshot,
                            telemetry,
                            replayScope)
                            .onItem().transformToUni(enforced ->
                                applyCachePolicy(step, enforced, contextSnapshot))
                            .onItem().invoke(output -> {
                                if (telemetry != null) {
                                    telemetry.recordReplayOutput(replayScope, output);
                                }
                            });
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true, replayScope);
                    })
                    .merge(maxConcurrency);
            }
            logger.debugf("Applying step %s (concatenate)", step.getClass());
            return multi
                .onItem()
                .transformToUni(item -> {
                    var replayScope = telemetry == null
                        ? null
                        : telemetry.beginReplayStep(step.getClass(), telemetryContext, true, item);
                    Uni<O> result = applyOneToOneWithCache(
                        step,
                        item,
                        cacheReadSupport,
                        contextSnapshot,
                        awaitContextSnapshot,
                        telemetry,
                        replayScope)
                        .onItem().transformToUni(enforced ->
                            applyCachePolicy(step, enforced, contextSnapshot))
                        .onItem().invoke(output -> {
                            if (telemetry != null) {
                                telemetry.recordReplayOutput(replayScope, output);
                            }
                        });
                    if (telemetry == null) {
                        return result;
                    }
                    result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                    return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true, replayScope);
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        Multi<I> input = (Multi<I>) current;
        if (telemetry != null) {
            input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
        }
        Multi<I> finalInput = scopedMultiInput(input, contextSnapshot, awaitContextSnapshot);
        Multi<O> result = withStepExecutionMulti(
            contextSnapshot,
            awaitContextSnapshot,
            () -> step.applyAwaitPerItem(finalInput));
        return telemetry == null
            ? result
            : telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
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
        PipelineTelemetry telemetry,
        Object replayScope) {
        if (cacheReadSupport == null) {
            return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        if (step instanceof CacheReadBypass) {
            return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        CachePolicy policy = cacheReadSupport.resolvePolicy(contextSnapshot);
        if (!cacheReadSupport.shouldRead(policy)) {
            return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        Class<?> targetType = null;
        if (step instanceof CacheKeyTarget cacheKeyTarget) {
            targetType = cacheKeyTarget.cacheKeyTargetType();
        }
        java.util.Optional<String> resolvedKey = cacheReadSupport.resolveKey(item, contextSnapshot, targetType);
        if (resolvedKey.isEmpty()) {
            if (policy == CachePolicy.REQUIRE_CACHE) {
                return withPipelineContext(contextSnapshot, () -> Uni.createFrom().failure(
                    new IllegalStateException("Cache key required but could not be resolved")));
            }
            return withPipelineContext(contextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.MISS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        String key = cacheReadSupport.withVersionPrefix(resolvedKey.get(), contextSnapshot);
        return cacheReadSupport.reader().get(key)
            .onItemOrFailure().transformToUni((cached, failure) -> {
                if (failure != null) {
                    if (policy == CachePolicy.REQUIRE_CACHE) {
                        return Uni.createFrom().failure(failure);
                    }
                    return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                        PipelineCacheStatusHolder.set(CacheStatus.MISS);
                        return step.apply(Uni.createFrom().item(item));
                    });
                }
                if (cached.isPresent()) {
                    return withPipelineContext(contextSnapshot, () -> {
                        PipelineCacheStatusHolder.set(CacheStatus.HIT);
                        if (telemetry != null) {
                            telemetry.recordReplayCacheHit(replayScope);
                        }
                        try {
                            @SuppressWarnings("unchecked")
                            O value = (O) cached.get();
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
                return withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> {
                    PipelineCacheStatusHolder.set(CacheStatus.MISS);
                    return step.apply(Uni.createFrom().item(item));
                });
            });
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

    @SuppressWarnings("unchecked")
    static <I, O> Object applyOneToOneFutureUnchecked(
        StepOneToOneCompletableFuture<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<I> finalInput = input;
            var replayScope = telemetry == null
                ? null
                : telemetry.beginPendingReplayStep(step.getClass(), telemetryContext, false);
            if (telemetry != null) {
                finalInput = finalInput.onItem().invoke(item -> telemetry.recordReplayInput(replayScope, item));
            }
            Uni<I> replayInput = scopedUniInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> step.apply(replayInput))
                .onItem().invoke(output -> {
                    if (telemetry != null) {
                        telemetry.recordReplayOutput(replayScope, output);
                    }
                });
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false, replayScope);
        } else if (current instanceof Multi<?>) {
            Multi<I> multi = (Multi<I>) current;
            if (telemetry != null) {
                multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
            }
            if (parallel) {
                return multi
                    .onItem()
                    .transformToUni(item -> {
                        var replayScope = telemetry == null
                            ? null
                            : telemetry.beginReplayStep(step.getClass(), telemetryContext, true, item);
                        Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)))
                            .onItem().invoke(output -> {
                                if (telemetry != null) {
                                    telemetry.recordReplayOutput(replayScope, output);
                                }
                            });
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true, replayScope);
                    })
                    .merge(maxConcurrency);
            }
            return multi
                .onItem()
                .transformToUni(item -> {
                    var replayScope = telemetry == null
                        ? null
                        : telemetry.beginReplayStep(step.getClass(), telemetryContext, true, item);
                    Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot,
                        () -> step.apply(Uni.createFrom().item(item)))
                        .onItem().invoke(output -> {
                            if (telemetry != null) {
                                telemetry.recordReplayOutput(replayScope, output);
                            }
                        });
                    if (telemetry == null) {
                        return result;
                    }
                    result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                    return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true, replayScope);
                })
                .concatenate();
        }
        throw new IllegalArgumentException(MessageFormat.format(
            "Unsupported current type for StepOneToOneCompletableFuture: {0}",
            current == null ? "null" : current.getClass().getName()));
    }

    @SuppressWarnings({"unchecked"})
    static <I, O> Object applyOneToManyUnchecked(
        StepOneToMany<I, O> step,
        Object current,
        boolean parallel,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            Uni<I> finalInput = input;
            var replayScope = telemetry == null
                ? null
                : telemetry.beginPendingReplayStep(step.getClass(), telemetryContext, false);
            if (telemetry != null) {
                finalInput = finalInput.onItem().invoke(item -> telemetry.recordReplayInput(replayScope, item));
            }
            Uni<I> replayInput = scopedUniInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Multi<O> result = withStepExecutionMulti(
                contextSnapshot,
                awaitContextSnapshot,
                () -> awaitContextSnapshot == null
                    ? step.apply(replayInput)
                    : applyOneToManyForAwaitParent(step, replayInput))
                .onItem().invoke(output -> {
                    if (telemetry != null) {
                        telemetry.recordReplayOutput(replayScope, output);
                    }
                });
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, false, replayScope);
        } else if (current instanceof Multi<?>) {
            Multi<I> multi = (Multi<I>) current;
            if (telemetry != null) {
                multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
            }
            if (parallel) {
                logger.debugf("Applying step %s (merge)", step.getClass());
                return multi
                    .onItem()
                    .transformToMulti(item -> {
                        var replayScope = telemetry == null
                            ? null
                            : telemetry.beginReplayStep(step.getClass(), telemetryContext, true, item);
                        Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot,
                            () -> step.apply(Uni.createFrom().item(item)))
                            .onItem().invoke(output -> {
                                if (telemetry != null) {
                                    telemetry.recordReplayOutput(replayScope, output);
                                }
                            });
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true, replayScope);
                    })
                    .merge(maxConcurrency);
            }
            logger.debugf("Applying step %s (concatenate)", step.getClass());
            return multi
                .onItem()
                .transformToMulti(item -> {
                    var replayScope = telemetry == null
                        ? null
                        : telemetry.beginReplayStep(step.getClass(), telemetryContext, true, item);
                    Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot,
                        () -> step.apply(Uni.createFrom().item(item)))
                        .onItem().invoke(output -> {
                            if (telemetry != null) {
                                telemetry.recordReplayOutput(replayScope, output);
                            }
                        });
                    if (telemetry == null) {
                        return result;
                    }
                    result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                    return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true, replayScope);
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
            .invoke(t -> PipelineTelemetry.recordRetry(step.getClass(), t))
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        return applyManyToOneUnchecked(step, current, telemetry, telemetryContext, contextSnapshot, null);
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToOneUnchecked(
        ManyToOne<I, O> step,
        Object current,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Multi<?>) {
            Multi<I> input = (Multi<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            var replayScope = telemetry == null
                ? null
                : telemetry.beginPendingReplayStep(step.getClass(), telemetryContext, false);
            Multi<I> finalInput = telemetry == null
                ? input
                : input.onItem().invoke(item -> telemetry.recordReplayInput(replayScope, item));
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> {
                    if (telemetry != null) {
                        telemetry.recordReplayOutput(replayScope, output);
                    }
                });
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false, replayScope);
        } else if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            var replayScope = telemetry == null
                ? null
                : telemetry.beginPendingReplayStep(step.getClass(), telemetryContext, false);
            Multi<I> finalInput = telemetry == null
                ? input.toMulti()
                : input.onItem().invoke(item -> telemetry.recordReplayInput(replayScope, item)).toMulti();
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Uni<O> result = withStepExecutionUni(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> {
                    if (telemetry != null) {
                        telemetry.recordReplayOutput(replayScope, output);
                    }
                });
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, false, replayScope);
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
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        return applyManyToManyUnchecked(step, current, telemetry, telemetryContext, contextSnapshot, null);
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToManyUnchecked(
        StepManyToMany<I, O> step,
        Object current,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot,
        AwaitExecutionContext awaitContextSnapshot) {
        if (current instanceof Uni<?>) {
            Uni<I> input = (Uni<I>) current;
            if (telemetry != null) {
                input = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, input);
            }
            var replayScope = telemetry == null
                ? null
                : telemetry.beginPendingReplayStep(step.getClass(), telemetryContext, false);
            Multi<I> finalInput = telemetry == null
                ? input.toMulti()
                : input.onItem().invoke(item -> telemetry.recordReplayInput(replayScope, item)).toMulti();
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> {
                    if (telemetry != null) {
                        telemetry.recordReplayOutput(replayScope, output);
                    }
                });
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, false, replayScope);
        } else if (current instanceof Multi<?> input) {
            logger.debugf("Applying many-to-many step %s on full stream", step.getClass());
            Multi<I> typedInput = (Multi<I>) input;
            if (telemetry != null) {
                typedInput = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, typedInput);
            }
            var replayScope = telemetry == null
                ? null
                : telemetry.beginPendingReplayStep(step.getClass(), telemetryContext, false);
            Multi<I> finalInput = telemetry == null
                ? typedInput
                : typedInput.onItem().invoke(item -> telemetry.recordReplayInput(replayScope, item));
            Multi<I> scopedInput = scopedMultiInput(finalInput, contextSnapshot, awaitContextSnapshot);
            Multi<O> result = withStepExecutionMulti(contextSnapshot, awaitContextSnapshot, () -> step.apply(scopedInput))
                .onItem().invoke(output -> {
                    if (telemetry != null) {
                        telemetry.recordReplayOutput(replayScope, output);
                    }
                });
            if (telemetry == null) {
                return result;
            }
            result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
            return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, false, replayScope);
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
