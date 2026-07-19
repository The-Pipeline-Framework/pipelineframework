package org.pipelineframework.awaitable;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/**
 * Runtime bridge used by generated await step beans.
 */
@ApplicationScoped
public class AwaitStepSupport {

    @Inject
    AwaitCoordinator awaitCoordinator;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    AwaitLiveCompletionRegistry liveCompletionRegistry;

    /**
     * Creates/dispatches an await interaction and suspends queue-async execution.
     */
    @SuppressWarnings("unchecked")
    public <I, O> Uni<O> awaitOneToOne(AwaitStepDescriptor descriptor, I input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Uni.createFrom().failure(e);
        }
        return awaitOneToOne(descriptor, input, context);
    }

    private <I, O> Uni<O> awaitOneToOne(AwaitStepDescriptor descriptor, I input, AwaitExecutionContext context) {
        int stepIndex = context.currentStepIndex();
        return withAwaitExecutionContext(context, () -> awaitCoordinator.createOrGet(
            descriptor,
            context.tenantId(),
            context.executionId(),
            stepIndex,
            context.executionId() + ":" + stepIndex,
            input,
            null,
            null)
            .onItem().transformToUni(created -> {
                AwaitInteractionRecord record = created.record();
                Uni<AwaitInteractionRecord> dispatched = record.status() == AwaitInteractionStatus.WAITING
                    ? awaitCoordinator.dispatch(descriptor, record)
                    : Uni.createFrom().item(record);
                return dispatched.onItem().transformToUni(updated ->
                    Uni.createFrom().failure(new AwaitSuspendedException(
                        context.tenantId(),
                        context.executionId(),
                        updated.unitId(),
                        stepIndex)));
            }));
    }

    /**
     * Resolves an await descriptor reactively before creating/dispatching the await interaction.
     */
    public <I, O> Uni<O> awaitOneToOne(Uni<AwaitStepDescriptor> descriptor, I input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Uni.createFrom().failure(e);
        }
        return descriptor.onItem().transformToUni(resolved -> awaitOneToOne(resolved, input, context));
    }

    /**
     * Creates/dispatches a single await interaction whose completion payload is replayed as a stream.
     */
    public <I, O> Multi<O> awaitOneToMany(Uni<AwaitStepDescriptor> descriptor, I input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Multi.createFrom().failure(e);
        }
        return descriptor
            .onItem().transformToMulti(resolved -> this.<I, O>awaitOneToOne(resolved, input, context).toMulti());
    }

    public <I, O> Multi<O> awaitOneToMany(AwaitStepDescriptor descriptor, I input) {
        return this.<I, O>awaitOneToOne(descriptor, input).toMulti();
    }

    /**
     * Creates one unary await interaction per upstream item and suspends after the upstream stream
     * has been fully dispatched.
     */
    public <I, O> Multi<O> awaitOneToOneStream(Uni<AwaitStepDescriptor> descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Multi.createFrom().failure(e);
        }
        return descriptor.onItem().transformToMulti(resolved -> awaitOneToOneStream(resolved, input, context));
    }

    public <I, O> Multi<O> awaitOneToOneStream(AwaitStepDescriptor descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Multi.createFrom().failure(e);
        }
        return awaitOneToOneStream(descriptor, input, context);
    }

    /**
     * Materializes the upstream batch and creates/dispatches one await interaction.
     */
    public <I, O> Uni<O> awaitManyToOne(Uni<AwaitStepDescriptor> descriptor, Multi<I> input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Uni.createFrom().failure(e);
        }
        return descriptor.onItem().transformToUni(resolved -> this.<I, O>awaitManyToOne(resolved, input, context));
    }

    public <I, O> Uni<O> awaitManyToOne(AwaitStepDescriptor descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Uni.createFrom().failure(e);
        }
        return awaitManyToOne(descriptor, input, context);
    }

    private <I, O> Uni<O> awaitManyToOne(AwaitStepDescriptor descriptor, Multi<I> input, AwaitExecutionContext context) {
        return materializeAggregateInput(descriptor, input)
            .onItem().transformToUni(materialized -> awaitOneToOne(descriptor, materialized, context));
    }

    /**
     * Materializes the upstream batch, dispatches one aggregate await interaction, and replays the
     * completion payload as one materialized output unit.
     */
    public <I, O> Multi<O> awaitManyToMany(Uni<AwaitStepDescriptor> descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Multi.createFrom().failure(e);
        }
        return descriptor
            .onItem().transformToMulti(resolved -> this.<I, O>awaitManyToMany(resolved, input, context));
    }

    public <I, O> Multi<O> awaitManyToMany(AwaitStepDescriptor descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Multi.createFrom().failure(e);
        }
        return awaitManyToMany(descriptor, input, context);
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitManyToMany(AwaitStepDescriptor descriptor, Multi<I> input, AwaitExecutionContext context) {
        return materializeAggregateInput(descriptor, input)
            .onItem().transformToMulti(materialized -> {
                if (materialized.isEmpty()) {
                    return Multi.createFrom().<O>empty();
                }
                return this.<List<I>, Object>awaitOneToOne(descriptor, materialized, context)
                    .toMulti()
                    .onItem().transformToMultiAndConcatenate(result ->
                        result instanceof Iterable
                            ? Multi.createFrom().<O>iterable((Iterable<O>) result)
                            : Multi.createFrom().<O>item((O) result));
            });
    }

    private <T> Uni<List<T>> materializeAggregateInput(AwaitStepDescriptor descriptor, Multi<T> input) {
        int configuredLimit = orchestratorConfig.awaitAggregateMaxInputItems();
        java.util.function.Supplier<List<T>> supplier = ArrayList::new;
        java.util.function.BiConsumer<List<T>, T> accumulator = (items, item) -> {
            items.add(item);
            enforceAggregateItemLimit("input", descriptor, items.size(), configuredLimit);
        };
        return input.collect().in(supplier, accumulator).onItem().transform(List::copyOf);
    }

    private AwaitExecutionContext captureExecutionContext() {
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            throw new IllegalStateException("Await steps require pipeline.orchestrator.mode=QUEUE_ASYNC.");
        }
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        if (context == null) {
            throw new IllegalStateException("Await step executed without queue-async execution context.");
        }
        return new AwaitExecutionContext(context.tenantId(), context.executionId(), context.currentStepIndex());
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneStream(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context
    ) {
        if (awaitCoordinator.supportsLiveAwaitWindow(descriptor)) {
            return awaitOneToOneLiveStream(descriptor, input, context);
        }
        return awaitOneToOneStreamSuspending(descriptor, input, context);
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneLiveStream(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context
    ) {
        int stepIndex = context.currentStepIndex();
        String unitId = streamUnitId(descriptor, context, stepIndex);
        return Multi.createFrom().deferred(() -> {
            AwaitLiveCompletionRegistry.LiveAwaitSession<O> session;
            try {
                session = liveCompletionRegistry.open(descriptor, context.tenantId(), unitId);
            } catch (Throwable failure) {
                return Multi.createFrom().failure(failure);
            }
            AtomicInteger itemIndex = new AtomicInteger();
            AtomicReference<Cancellable> dispatchSubscription = new AtomicReference<>();
            Uni<Void> dispatch = dispatchLiveAwaitItems(descriptor, input, context, unitId, itemIndex, session)
                .onItem().transformToUni(ignored -> {
                    int dispatchedItems = itemIndex.get();
                    if (dispatchedItems == 0) {
                        session.markDispatchComplete(0);
                        return Uni.createFrom().voidItem();
                    }
                    return awaitCoordinator.markDispatchComplete(
                        context.tenantId(),
                        unitId,
                        dispatchedItems,
                        System.currentTimeMillis())
                        .invoke(unit -> session.markDispatchComplete(dispatchedItems))
                        .replaceWithVoid();
                })
                .onFailure().invoke(session::fail)
                .replaceWithVoid();
            return Multi.createFrom().publisher(session)
                .onSubscription().invoke(ignored -> dispatchSubscription.set(dispatch.subscribe().with(
                    item -> {
                    },
                    session::fail)))
                .onTermination().invoke((failure, cancelled) -> {
                    Cancellable active = dispatchSubscription.get();
                    if (cancelled && active != null) {
                        active.cancel();
                    }
                    if (failure != null) {
                        session.fail(failure);
                    }
                    liveCompletionRegistry.close(context.tenantId(), unitId);
                });
        });
    }

    private <I, O> Uni<Void> dispatchLiveAwaitItems(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context,
        String unitId,
        AtomicInteger itemIndex,
        AwaitLiveCompletionRegistry.LiveAwaitSession<O> session
    ) {
        java.util.function.Function<I, Uni<? extends AwaitInteractionRecord>> itemDispatch = item -> {
            int index = itemIndex.getAndIncrement();
            return dispatchLiveAwaitItem(descriptor, item, context, unitId, index, session);
        };
        Multi<AwaitInteractionRecord> dispatches = pipelineConfig != null && pipelineConfig.parallelism() == ParallelismPolicy.SEQUENTIAL
            ? input.onItem().transformToUni(itemDispatch).concatenate()
            : input.onItem().transformToUni(itemDispatch).merge(liveAwaitPendingWindow());
        return dispatches.collect().in(() -> Boolean.TRUE, (ignored, record) -> {
        }).replaceWithVoid();
    }

    private <I, O> Uni<AwaitInteractionRecord> dispatchLiveAwaitItem(
        AwaitStepDescriptor descriptor,
        I item,
        AwaitExecutionContext context,
        String unitId,
        int index,
        AwaitLiveCompletionRegistry.LiveAwaitSession<O> session
    ) {
        String completionKey = "item:" + index;
        return session.acquirePermit(completionKey, liveAwaitPendingWindow())
            .chain(() -> withAwaitExecutionContext(context, () -> awaitCoordinator.createOrGetItem(
            descriptor,
            context.tenantId(),
            context.executionId(),
            context.currentStepIndex(),
            context.executionId() + ":" + context.currentStepIndex() + ":" + index,
            item,
            unitId,
            index,
            null,
            null)
            .onItem().transformToUni(created -> {
                AwaitInteractionRecord record = created.record();
                if (record.status() == AwaitInteractionStatus.COMPLETED) {
                    return session.accept(record).replaceWith(record);
                }
                if (record.status().terminal()) {
                    return Uni.createFrom().failure(new IllegalStateException(
                        "Await interaction " + record.interactionId()
                            + " is terminal with status " + record.status()
                            + " and cannot be accepted by the live await stream."));
                }
                if (record.status() == AwaitInteractionStatus.WAITING) {
                    return awaitCoordinator.dispatch(descriptor, record);
                }
                return Uni.createFrom().item(record);
            })));
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneStreamSuspending(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context
    ) {
        int stepIndex = context.currentStepIndex();
        String unitId = streamUnitId(descriptor, context, stepIndex);
        AtomicInteger itemIndex = new AtomicInteger();
        Multi<AwaitInteractionRecord> dispatched = input.onItem().transformToUniAndConcatenate(item -> {
            int index = itemIndex.getAndIncrement();
            return withAwaitExecutionContext(context, () -> awaitCoordinator.createOrGetItem(
                descriptor,
                context.tenantId(),
                context.executionId(),
                stepIndex,
                context.executionId() + ":" + stepIndex + ":" + index,
                item,
                unitId,
                index,
                null,
                null)
                .onItem().transformToUni(created -> {
                    AwaitInteractionRecord record = created.record();
                    return record.status() == AwaitInteractionStatus.WAITING
                        ? awaitCoordinator.dispatch(descriptor, record)
                        : Uni.createFrom().item(record);
                }));
        });
        return dispatched
            .collect().in(() -> Boolean.TRUE, (ignored, record) -> {
            })
            .onItem().transformToMulti(ignored -> {
                if (itemIndex.get() == 0) {
                    return Multi.createFrom().empty();
                }
                return awaitCoordinator.markDispatchComplete(
                    context.tenantId(),
                    unitId,
                    itemIndex.get(),
                    System.currentTimeMillis())
                    .onItem().transformToMulti(unit -> unit.status() == AwaitUnitStatus.COMPLETED
                        ? awaitCoordinator.loadResumePayload(context.tenantId(), unitId)
                            .toMulti()
                            .onItem().transformToMultiAndConcatenate(payload -> {
                                if (payload instanceof Iterable) {
                                    return Multi.createFrom().<O>iterable((Iterable<O>) payload);
                                } else if (payload != null && payload.getClass().isArray()) {
                                    int length = Array.getLength(payload);
                                    List<O> items = new ArrayList<>(length);
                                    for (int i = 0; i < length; i++) {
                                        items.add((O) Array.get(payload, i));
                                    }
                                    return Multi.createFrom().iterable(items);
                                } else {
                                    return Multi.createFrom().<O>item((O) payload);
                                }
                            })
                        : Uni.createFrom().<O>failure(new AwaitSuspendedException(
                            context.tenantId(),
                            context.executionId(),
                            unitId,
                            stepIndex)).toMulti());
            });
    }

    private static String streamUnitId(AwaitStepDescriptor descriptor, AwaitExecutionContext context, int stepIndex) {
        return UUID.nameUUIDFromBytes((context.tenantId() + ":" + context.executionId() + ":"
            + descriptor.stepId() + ":" + stepIndex).getBytes(StandardCharsets.UTF_8)).toString();
    }

    private int awaitMaxConcurrency() {
        int configured = pipelineConfig == null ? 128 : pipelineConfig.maxConcurrency();
        if (configured < 1) {
            return 1;
        }
        return Math.min(configured, 1024);
    }

    private int liveAwaitPendingWindow() {
        return pipelineConfig != null && pipelineConfig.parallelism() == ParallelismPolicy.SEQUENTIAL
            ? 1
            : awaitMaxConcurrency();
    }

    private <T> Uni<T> withAwaitExecutionContext(AwaitExecutionContext context, java.util.function.Supplier<Uni<T>> supplier) {
        return Uni.createFrom().deferred(() -> {
            AwaitExecutionContext previous = AwaitExecutionContextHolder.get();
            AwaitExecutionContextHolder.set(context);
            try {
                return supplier.get().onTermination().invoke(() -> restoreAwaitExecutionContext(previous));
            } catch (Throwable failure) {
                restoreAwaitExecutionContext(previous);
                return Uni.createFrom().failure(failure);
            }
        });
    }

    private void restoreAwaitExecutionContext(AwaitExecutionContext previous) {
        if (previous == null) {
            AwaitExecutionContextHolder.clear();
        } else {
            AwaitExecutionContextHolder.set(previous);
        }
    }

    private static void enforceAggregateItemLimit(
        String materializedSide,
        AwaitStepDescriptor descriptor,
        int itemCount,
        int configuredLimit
    ) {
        if (configuredLimit <= 0 || itemCount <= configuredLimit) {
            return;
        }
        throw new IllegalStateException(
            "Await step " + descriptor.stepId()
                + " materialized " + itemCount + " " + materializedSide
                + " items for " + descriptor.cardinality()
                + ", exceeding pipeline.orchestrator.await-aggregate-max-" + materializedSide
                + "-items=" + configuredLimit + ".");
    }
}
