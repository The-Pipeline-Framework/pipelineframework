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
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.CacheKeyTarget;
import org.pipelineframework.cache.CachePolicy;
import org.pipelineframework.cache.CachePolicyEnforcer;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.cache.CacheReadBypass;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.step.future.StepOneToOneCompletableFuture;
import org.pipelineframework.telemetry.PipelineTelemetry;

@ApplicationScoped
class PipelineStepExecutor {

    private static final Logger logger = Logger.getLogger(PipelineStepExecutor.class);

    Object applyStep(
        Object step,
        Object current,
        PipelineParallelismPolicyResolver parallelismPolicyResolver,
        org.pipelineframework.config.ParallelismPolicy parallelismPolicy,
        int maxConcurrency,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineRunner.CacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot) {
        return switch (step) {
            case StepOneToOne<?, ?> stepOneToOne -> {
                boolean parallel = parallelismPolicyResolver.shouldParallelize(
                    stepOneToOne,
                    parallelismPolicy,
                    PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE);
                yield applyOneToOneUnchecked(stepOneToOne, current, parallel, maxConcurrency, telemetry, telemetryContext, cacheReadSupport,
                    contextSnapshot);
            }
            case StepOneToOneCompletableFuture<?, ?> stepFuture -> {
                boolean parallel = parallelismPolicyResolver.shouldParallelize(
                    stepFuture,
                    parallelismPolicy,
                    PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_ONE_FUTURE);
                yield applyOneToOneFutureUnchecked(stepFuture, current, parallel, maxConcurrency, telemetry, telemetryContext,
                    contextSnapshot);
            }
            case StepOneToMany<?, ?> stepOneToMany -> {
                boolean parallel = parallelismPolicyResolver.shouldParallelize(
                    stepOneToMany,
                    parallelismPolicy,
                    PipelineParallelismPolicyResolver.StepParallelismType.ONE_TO_MANY);
                yield applyOneToManyUnchecked(stepOneToMany, current, parallel, maxConcurrency, telemetry, telemetryContext,
                    contextSnapshot);
            }
            case ManyToOne<?, ?> manyToOne -> applyManyToOneUnchecked(manyToOne, current, telemetry, telemetryContext, contextSnapshot);
            case StepManyToMany<?, ?> manyToMany -> applyManyToManyUnchecked(manyToMany, current, telemetry, telemetryContext,
                contextSnapshot);
            default -> {
                logger.errorf("Step not recognised: %s", step.getClass().getName());
                throw new IllegalArgumentException("Step not recognised: " + step.getClass().getName());
            }
        };
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
            }
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
        throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepOneToOne: {0}", current));
    }

    private static <I, O> Uni<O> applyOneToOneWithCache(
        StepOneToOne<I, O> step,
        I item,
        PipelineCacheReadSupport cacheReadSupport,
        PipelineContext contextSnapshot) {
        if (cacheReadSupport == null) {
            return withPipelineContext(contextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        if (step instanceof CacheReadBypass) {
            return withPipelineContext(contextSnapshot, () -> {
                PipelineCacheStatusHolder.set(CacheStatus.BYPASS);
                return step.apply(Uni.createFrom().item(item));
            });
        }
        CachePolicy policy = cacheReadSupport.resolvePolicy(contextSnapshot);
        if (!cacheReadSupport.shouldRead(policy)) {
            return withPipelineContext(contextSnapshot, () -> {
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
        return cacheReadSupport.reader.get(key)
            .onItemOrFailure().transformToUni((cached, failure) -> {
                if (failure != null) {
                    if (policy == CachePolicy.REQUIRE_CACHE) {
                        return Uni.createFrom().failure(failure);
                    }
                    return withPipelineContext(contextSnapshot, () -> {
                        PipelineCacheStatusHolder.set(CacheStatus.MISS);
                        return step.apply(Uni.createFrom().item(item));
                    });
                }
                if (cached.isPresent()) {
                    return withPipelineContext(contextSnapshot, () -> {
                        PipelineCacheStatusHolder.set(CacheStatus.HIT);
                        @SuppressWarnings("unchecked")
                        O value = (O) cached.get();
                        return Uni.createFrom().item(value);
                    });
                }
                if (policy == CachePolicy.REQUIRE_CACHE) {
                    return Uni.createFrom().failure(new CachePolicyViolation(
                        "Cache policy REQUIRE_CACHE failed for key: " + key));
                }
                return withPipelineContext(contextSnapshot, () -> {
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

    @SuppressWarnings("unchecked")
    static <I, O> Object applyOneToOneFutureUnchecked(
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
                        Uni<O> result = withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true);
                    })
                    .merge(maxConcurrency);
            }
            return multi
                .onItem()
                .transformToUni(item -> {
                    Uni<O> result = withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
                    if (telemetry == null) {
                        return result;
                    }
                    result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                    return telemetry.instrumentStepUni(step.getClass(), result, telemetryContext, true);
                })
                .concatenate();
        }
        throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepOneToOneCompletableFuture: {0}", current));
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
            Multi<I> multi = (Multi<I>) current;
            if (telemetry != null) {
                multi = telemetry.instrumentItemConsumed(step.getClass(), telemetryContext, multi);
            }
            if (parallel) {
                logger.debugf("Applying step %s (merge)", step.getClass());
                return multi
                    .onItem()
                    .transformToMulti(item -> {
                        Multi<O> result = withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
                        if (telemetry == null) {
                            return result;
                        }
                        result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                        return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true);
                    })
                    .merge(maxConcurrency);
            }
            logger.debugf("Applying step %s (concatenate)", step.getClass());
            return multi
                .onItem()
                .transformToMulti(item -> {
                    Multi<O> result = withPipelineContext(contextSnapshot, () -> step.apply(Uni.createFrom().item(item)));
                    if (telemetry == null) {
                        return result;
                    }
                    result = telemetry.instrumentItemProduced(step.getClass(), telemetryContext, result);
                    return telemetry.instrumentStepMulti(step.getClass(), result, telemetryContext, true);
                })
                .concatenate();
        }
        throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepOneToMany: {0}", current));
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToOneUnchecked(
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
        }
        throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepManyToOne: {0}", current));
    }

    @SuppressWarnings("unchecked")
    static <I, O> Object applyManyToManyUnchecked(
        StepManyToMany<I, O> step,
        Object current,
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext telemetryContext,
        PipelineContext contextSnapshot) {
        if (current instanceof Uni<?>) {
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
        } else if (current instanceof Multi<?> multi) {
            logger.debugf("Applying many-to-many step %s on full stream", step.getClass());
            Multi<I> input = (Multi<I>) multi;
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
        }
        throw new IllegalArgumentException(MessageFormat.format("Unsupported current type for StepManyToMany: {0}", current));
    }
}
