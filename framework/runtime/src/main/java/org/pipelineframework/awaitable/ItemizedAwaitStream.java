package org.pipelineframework.awaitable;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;

final class ItemizedAwaitStream {

    private final AwaitCoordinator awaitCoordinator;
    private final AwaitLiveCompletionRegistry liveCompletionRegistry;
    private final PipelineConfig pipelineConfig;

    ItemizedAwaitStream(
        AwaitCoordinator awaitCoordinator,
        AwaitLiveCompletionRegistry liveCompletionRegistry,
        PipelineConfig pipelineConfig) {
        this.awaitCoordinator = awaitCoordinator;
        this.liveCompletionRegistry = liveCompletionRegistry;
        this.pipelineConfig = pipelineConfig;
    }

    @SuppressWarnings("unchecked")
    <I, O> Multi<O> awaitOneToOneStream(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context) {
        if (isKafkaItemStream(descriptor)) {
            return awaitOneToOneKafkaLiveStream(descriptor, input, context);
        }
        return awaitOneToOneStreamSuspending(descriptor, input, context);
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneKafkaLiveStream(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context) {
        int stepIndex = context.currentStepIndex();
        String unitId = streamUnitId(descriptor, context, stepIndex);
        return Multi.createFrom().deferred(() -> openKafkaLiveSession(descriptor, input, context, unitId));
    }

    private <I, O> Multi<O> openKafkaLiveSession(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context,
        String unitId) {
        LiveAwaitSession<O> session;
        try {
            session = liveCompletionRegistry.open(descriptor, context.tenantId(), unitId);
        } catch (Throwable failure) {
            return Multi.createFrom().failure(failure);
        }
        AtomicInteger itemIndex = new AtomicInteger();
        Uni<Void> dispatch = dispatchLiveKafkaAwaitItems(descriptor, input, context, unitId, itemIndex, session)
            .chain(() -> markLiveDispatchComplete(context, unitId, itemIndex.get(), session))
            .onFailure().invoke(session::fail)
            .replaceWithVoid();
        return liveSessionPublisher(context, unitId, session, dispatch);
    }

    private <O> Uni<Void> markLiveDispatchComplete(
        AwaitExecutionContext context,
        String unitId,
        int dispatchedItems,
        LiveAwaitSession<O> session) {
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
    }

    private <O> Multi<O> liveSessionPublisher(
        AwaitExecutionContext context,
        String unitId,
        LiveAwaitSession<O> session,
        Uni<Void> dispatch) {
        AtomicReference<Cancellable> dispatchSubscription = new AtomicReference<>();
        return Multi.createFrom().publisher(session)
            .onSubscription().invoke(ignored -> dispatchSubscription.set(dispatch.subscribe().with(
                item -> {
                },
                session::fail)))
            .onTermination().invoke((failure, cancelled) ->
                closeLiveSession(context, unitId, session, dispatchSubscription.get(), failure, cancelled));
    }

    private <O> void closeLiveSession(
        AwaitExecutionContext context,
        String unitId,
        LiveAwaitSession<O> session,
        Cancellable dispatchSubscription,
        Throwable failure,
        boolean cancelled) {
        if ((cancelled || failure != null) && dispatchSubscription != null) {
            dispatchSubscription.cancel();
        }
        if (failure != null) {
            session.fail(failure);
        }
        liveCompletionRegistry.close(context.tenantId(), unitId, session);
    }

    private <I, O> Uni<Void> dispatchLiveKafkaAwaitItems(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context,
        String unitId,
        AtomicInteger itemIndex,
        LiveAwaitSession<O> session) {
        Multi<AwaitInteractionRecord> dispatches = input.onItem().transformToUni(item -> {
            int index = itemIndex.getAndIncrement();
            return dispatchLiveKafkaAwaitItem(descriptor, item, context, unitId, index, session);
        }).merge(awaitMaxConcurrency());
        if (pipelineConfig != null && pipelineConfig.parallelism() == ParallelismPolicy.SEQUENTIAL) {
            dispatches = input.onItem().transformToUni(item -> {
                int index = itemIndex.getAndIncrement();
                return dispatchLiveKafkaAwaitItem(descriptor, item, context, unitId, index, session);
            }).concatenate();
        }
        return dispatches.collect().in(() -> Boolean.TRUE, (ignored, record) -> {
        }).replaceWithVoid();
    }

    private <I, O> Uni<AwaitInteractionRecord> dispatchLiveKafkaAwaitItem(
        AwaitStepDescriptor descriptor,
        I item,
        AwaitExecutionContext context,
        String unitId,
        int index,
        LiveAwaitSession<O> session) {
        return withAwaitExecutionContext(context, () -> awaitCoordinator.createOrGetItem(
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
                Uni<Void> accepted = session.awaitAccepted(record);
                if (record.status() == AwaitInteractionStatus.WAITING) {
                    return awaitCoordinator.dispatch(descriptor, record)
                        .chain(dispatched -> accepted.replaceWith(dispatched));
                }
                return accepted.replaceWith(record);
            }));
    }

    @SuppressWarnings("unchecked")
    private <I, O> Multi<O> awaitOneToOneStreamSuspending(
        AwaitStepDescriptor descriptor,
        Multi<I> input,
        AwaitExecutionContext context) {
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

    private static boolean isKafkaItemStream(AwaitStepDescriptor descriptor) {
        return descriptor != null
            && "ONE_TO_ONE".equalsIgnoreCase(descriptor.cardinality())
            && "kafka".equalsIgnoreCase(descriptor.transportType());
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

    private <T> Uni<T> withAwaitExecutionContext(
        AwaitExecutionContext context,
        java.util.function.Supplier<Uni<T>> supplier) {
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
