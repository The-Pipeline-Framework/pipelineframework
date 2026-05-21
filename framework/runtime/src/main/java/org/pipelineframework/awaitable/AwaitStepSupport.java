package org.pipelineframework.awaitable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
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

    /**
     * Creates/dispatches an await interaction and suspends queue-async execution.
     */
    @SuppressWarnings("unchecked")
    public <I, O> Uni<O> awaitOneToOne(AwaitStepDescriptor descriptor, I input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Await steps require pipeline.orchestrator.mode=QUEUE_ASYNC."));
        }
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        if (context == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Await step executed without queue-async execution context."));
        }
        int stepIndex = context.currentStepIndex();
        return awaitCoordinator.createOrGet(
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
                        updated.interactionId(),
                        stepIndex)));
            });
    }

    /**
     * Resolves an await descriptor reactively before creating/dispatching the await interaction.
     */
    public <I, O> Uni<O> awaitOneToOne(Uni<AwaitStepDescriptor> descriptor, I input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        return descriptor.onItem().transformToUni(resolved -> awaitOneToOne(resolved, input));
    }

    /**
     * Creates/dispatches a single await interaction whose completion payload is replayed as a stream.
     */
    public <I, O> Multi<O> awaitOneToMany(Uni<AwaitStepDescriptor> descriptor, I input) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        return descriptor
            .onItem().transformToMulti(resolved -> this.<I, O>awaitOneToMany(resolved, input));
    }

    public <I, O> Multi<O> awaitOneToMany(AwaitStepDescriptor descriptor, I input) {
        return this.<I, O>awaitOneToOne(descriptor, input).toMulti();
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
        return descriptor.onItem().transformToUni(resolved -> this.<I, O>awaitManyToOne(resolved, input));
    }

    public <I, O> Uni<O> awaitManyToOne(AwaitStepDescriptor descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        return input.collect().asList()
            .onItem().transformToUni(items -> awaitOneToOne(descriptor, List.copyOf(items)));
    }

    /**
     * Creates one await interaction per upstream item and suspends until the barrier completes.
     */
    public <I, O> Multi<O> awaitManyToMany(Uni<AwaitStepDescriptor> descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        return descriptor
            .onItem().transformToMulti(resolved -> this.<I, O>awaitManyToMany(resolved, input));
    }

    public <I, O> Multi<O> awaitManyToMany(AwaitStepDescriptor descriptor, Multi<I> input) {
        if (descriptor == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (input == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        return input.collect().asList()
            .onItem().transformToMulti(items -> items.isEmpty()
                ? Multi.createFrom().<O>empty()
                : this.<I, O>awaitManyToMany(descriptor, List.copyOf(items)).toMulti());
    }

    private <I, O> Uni<O> awaitManyToMany(AwaitStepDescriptor descriptor, List<I> items) {
        if (!"per-item".equalsIgnoreCase(descriptor.dispatchMode())) {
            return Uni.createFrom().failure(new IllegalStateException(
                "MANY_TO_MANY await steps require dispatch.mode=per-item."));
        }
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Await steps require pipeline.orchestrator.mode=QUEUE_ASYNC."));
        }
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        if (context == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Await step executed without queue-async execution context."));
        }
        int stepIndex = context.currentStepIndex();
        String barrierId = UUID.nameUUIDFromBytes((context.tenantId() + ":" + context.executionId() + ":"
            + descriptor.stepId() + ":" + stepIndex).getBytes(StandardCharsets.UTF_8)).toString();
        List<Uni<AwaitInteractionRecord>> operations = new ArrayList<>(items.size());
        for (int index = 0; index < items.size(); index++) {
            I item = items.get(index);
            operations.add(awaitCoordinator.createOrGetBarrierItem(
                    descriptor,
                    context.tenantId(),
                    context.executionId(),
                    stepIndex,
                    context.executionId() + ":" + stepIndex + ":" + index,
                    item,
                    barrierId,
                    index,
                    items.size(),
                    null,
                    null)
                .onItem().transformToUni(created -> {
                    AwaitInteractionRecord record = created.record();
                    return record.status() == AwaitInteractionStatus.WAITING
                        ? awaitCoordinator.dispatch(descriptor, record)
                        : Uni.createFrom().item(record);
                }));
        }
        return Uni.join().all(operations).andCollectFailures()
            .onItem().transformToUni(ignored -> Uni.createFrom().failure(new AwaitSuspendedException(
                context.tenantId(),
                context.executionId(),
                barrierId,
                stepIndex)));
    }
}
