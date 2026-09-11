package org.pipelineframework.awaitable;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class AwaitCompletionSupport {

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
    public <I, O> Uni<O> awaitOneToOne(AwaitCompletionDescriptor descriptor, I input) {
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

    private <I, O> Uni<O> awaitOneToOne(AwaitCompletionDescriptor descriptor, I input, AwaitExecutionContext context) {
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
    public <I, O> Uni<O> awaitOneToOne(Uni<AwaitCompletionDescriptor> descriptor, I input) {
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
     * Creates one unary await interaction per upstream item and suspends after the upstream stream
     * has been fully dispatched.
     */
    public <I, O> Multi<O> awaitOneToOneStream(Uni<AwaitCompletionDescriptor> descriptor, Multi<I> input) {
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

    public <I, O> Multi<O> awaitOneToOneStream(AwaitCompletionDescriptor descriptor, Multi<I> input) {
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

    private AwaitExecutionContext captureExecutionContext() {
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            throw new IllegalStateException("Await steps require pipeline.orchestrator.mode=QUEUE_ASYNC.");
        }
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        if (context == null) {
            throw new IllegalStateException("Await step executed without queue-async execution context.");
        }
        return new AwaitExecutionContext(
            context.tenantId(),
            context.executionId(),
            context.currentStepIndex(),
            context.continuationMode(),
            context.terminalOutputOwnership(),
            context.traceMetadata());
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneStream(
        AwaitCompletionDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context
    ) {
        if (context.continuationMode() == AwaitContinuationMode.LIVE_IF_SUPPORTED
            && awaitCoordinator.supportsLiveAwaitWindow(descriptor)) {
            return awaitOneToOneLiveStream(descriptor, input, context);
        }
        return awaitOneToOneStreamSuspending(descriptor, input, context);
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneLiveStream(
        AwaitCompletionDescriptor descriptor,
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
            AtomicBoolean cancellationRequested = new AtomicBoolean();
            AtomicReference<Cancellable> dispatchSubscription = new AtomicReference<>();
            Uni<Void> dispatch = awaitCoordinator.preloadDurablePayloads(context.tenantId(), context.executionId())
                .chain(() -> awaitCoordinator.prepareLiveItemizedUnit(
                    descriptor, context.tenantId(), unitId, context.executionId(), stepIndex))
                .chain(() -> dispatchLiveAwaitItems(descriptor, input, context, unitId, itemIndex, session))
                .onItem().transformToUni(ignored -> {
                    int dispatchedItems = itemIndex.get();
                    session.markDispatchComplete(dispatchedItems);
                    return Uni.createFrom().voidItem();
                })
                .onFailure().invoke(session::fail)
                .replaceWithVoid();
            return Multi.createFrom().publisher(session)
                .onSubscription().invoke(ignored -> {
                    Cancellable active = dispatch.subscribe().with(item -> {
                    }, session::fail);
                    dispatchSubscription.set(active);
                    if (cancellationRequested.get()) {
                        active.cancel();
                    }
                })
                .onTermination().invoke((failure, wasCancelled) -> {
                    if (wasCancelled || failure != null) {
                        cancellationRequested.set(true);
                        Cancellable active = dispatchSubscription.get();
                        if (active != null) {
                            active.cancel();
                        }
                    }
                    if (failure != null) {
                        session.fail(failure);
                    }
                    liveCompletionRegistry.close(context.tenantId(), unitId);
                });
        });
    }

    private <I, O> Uni<Void> dispatchLiveAwaitItems(
        AwaitCompletionDescriptor descriptor,
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
        AwaitCompletionDescriptor descriptor,
        I item,
        AwaitExecutionContext context,
        String unitId,
        int index,
        AwaitLiveCompletionRegistry.LiveAwaitSession<O> session
    ) {
        String completionKey = "item:" + index;
        return session.acquirePermit(completionKey, liveAwaitPendingWindow())
            .chain(() -> withAwaitExecutionContext(context, () -> awaitCoordinator.createOrGetPreparedItem(
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
                if (record.status() == AwaitInteractionStatus.WAITING
                    || record.status() == AwaitInteractionStatus.DISPATCHING) {
                    return awaitCoordinator.dispatchLive(descriptor, record);
                }
                return Uni.createFrom().item(record);
            })));
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneStreamSuspending(
        AwaitCompletionDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context
    ) {
        int stepIndex = context.currentStepIndex();
        String unitId = streamUnitId(descriptor, context, stepIndex);
        AtomicInteger itemIndex = new AtomicInteger();
        java.util.function.Function<I, Uni<? extends AwaitInteractionRecord>> itemDispatch = item -> {
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
        };
        Multi<AwaitInteractionRecord> dispatched = pipelineConfig != null
                && pipelineConfig.parallelism() == ParallelismPolicy.SEQUENTIAL
            ? input.onItem().transformToUni(itemDispatch).concatenate()
            : input.onItem().transformToUni(itemDispatch).merge(awaitMaxConcurrency());
        return dispatched
            .collect().in(() -> Boolean.TRUE, (ignored, record) -> {
            })
            .onItem().transformToMulti(ignored -> {
                if (itemIndex.get() == 0) {
                    return Multi.createFrom().empty();
                }
                return awaitCoordinator.reconcileCompletedItemInteractions(
                    context.tenantId(),
                    unitId,
                    System.currentTimeMillis())
                    .chain(() -> awaitCoordinator.markDispatchComplete(
                        context.tenantId(),
                        unitId,
                        itemIndex.get(),
                        System.currentTimeMillis()))
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

    private static String streamUnitId(AwaitCompletionDescriptor descriptor, AwaitExecutionContext context, int stepIndex) {
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
                return supplier.get();
            } catch (Throwable failure) {
                return Uni.createFrom().failure(failure);
            } finally {
                restoreAwaitExecutionContext(previous);
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

}
