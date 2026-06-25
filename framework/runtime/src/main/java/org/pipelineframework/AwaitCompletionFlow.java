package org.pipelineframework;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionAdmissionFailures;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCompletionTenantMismatchException;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;

final class AwaitCompletionFlow {

    private final AwaitCoordinator awaitCoordinator;
    private final AwaitLiveCompletionRegistry liveCompletionRegistry;
    private final Effects effects;

    AwaitCompletionFlow(
        AwaitCoordinator awaitCoordinator,
        AwaitLiveCompletionRegistry liveCompletionRegistry,
        Effects effects) {
        this.awaitCoordinator = awaitCoordinator;
        this.liveCompletionRegistry = liveCompletionRegistry;
        this.effects = effects;
    }

    Uni<AwaitCompletionResult> complete(
        QueueAsyncCommand.CompleteAwait command,
        AwaitItemContinuationHandler itemContinuationHandler) {
        AwaitCompletionCommand normalized = command.command();
        return awaitCoordinator.complete(normalized)
            .onFailure(failure -> !AwaitCompletionAdmissionFailures.isDeterministic(failure))
            .transform(failure -> new IllegalStateException(
                "Failed completing await interaction tenant=" + normalized.tenantId()
                    + ", interactionId=" + normalized.interactionId()
                    + ", correlationId=" + normalized.correlationId(),
                failure))
            .onItem().transformToUni(result -> validateTenant(result, normalized)
                .onItem().transformToUni(validated -> awaitCoordinator
                    .recordCompletion(validated.record(), normalized.nowEpochMs())
                    .onItem().transformToUni(unit -> interpretLiveSignal(
                        new AwaitCompletionOutcome.SignalLiveSession(validated.record(), unit))
                        .onFailure().recoverWithItem(false)
                        .onItem().transformToUni(liveAccepted -> liveAccepted
                            ? Uni.createFrom().item(validated)
                            : handleRecordedCompletion(
                                validated,
                                unit,
                                itemContinuationHandler,
                                normalized.nowEpochMs())))));
    }

    private Uni<AwaitCompletionResult> handleRecordedCompletion(
        AwaitCompletionResult validated,
        AwaitUnitRecord unit,
        AwaitItemContinuationHandler itemContinuationHandler,
        long nowEpochMs) {
        AwaitInteractionRecord record = validated.record();
        if (ItemContinuationFlow.usesItemContinuations(record, unit)) {
            return effects.itemContinuationReady(record, unit)
                .onItem().invoke(ready -> interpretRecordedItemOutcome(
                    itemCompletionOutcome(record, unit, ready, nowEpochMs),
                    itemContinuationHandler,
                    record,
                    unit))
                .replaceWith(validated);
        }
        return interpretAggregateOutcome(aggregateCompletionOutcome(record, unit, nowEpochMs)).replaceWith(validated);
    }

    static AwaitCompletionOutcome itemCompletionOutcome(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        boolean ready,
        long nowEpochMs) {
        return ready
            ? new AwaitCompletionOutcome.DispatchItemContinuation(record, unit, nowEpochMs)
            : new AwaitCompletionOutcome.RecordOnly("itemized-await-parent-not-ready");
    }

    static AwaitCompletionOutcome aggregateCompletionOutcome(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        long nowEpochMs) {
        return unit.status() == AwaitUnitStatus.COMPLETED
            ? new AwaitCompletionOutcome.ReleaseParent(record, unit, nowEpochMs)
            : new AwaitCompletionOutcome.RecordOnly("await-unit-not-complete");
    }

    private Uni<Boolean> interpretLiveSignal(AwaitCompletionOutcome.SignalLiveSession decision) {
        if (liveCompletionRegistry == null) {
            return Uni.createFrom().item(false);
        }
        return liveCompletionRegistry.signal(decision.record(), decision.unit());
    }

    private void interpretRecordedItemOutcome(
        AwaitCompletionOutcome outcome,
        AwaitItemContinuationHandler itemContinuationHandler,
        AwaitInteractionRecord record,
        AwaitUnitRecord unit) {
        switch (outcome) {
            case AwaitCompletionOutcome.DispatchItemContinuation dispatch -> effects.dispatchItemContinuation(
                dispatch.record(),
                dispatch.unit(),
                itemContinuationHandler,
                dispatch.nowEpochMs());
            case AwaitCompletionOutcome.RecordOnly ignored -> effects.recordEarlyCompletionHeld(record, unit);
            default -> throw new IllegalStateException("Unsupported await item outcome: " + outcome);
        }
    }

    private Uni<Void> interpretAggregateOutcome(AwaitCompletionOutcome outcome) {
        return switch (outcome) {
            case AwaitCompletionOutcome.ReleaseParent release -> effects.releaseAwaitResume(
                release.record(),
                release.unit().unitId(),
                release.nowEpochMs());
            case AwaitCompletionOutcome.RecordOnly ignored -> Uni.createFrom().voidItem();
            default -> Uni.createFrom().failure(new IllegalStateException(
                "Unsupported aggregate await outcome: " + outcome));
        };
    }

    private Uni<AwaitCompletionResult> validateTenant(
        AwaitCompletionResult result,
        AwaitCompletionCommand command) {
        if (!command.tenantId().equals(result.record().tenantId())) {
            return Uni.createFrom().failure(new AwaitCompletionTenantMismatchException(
                "Await completion tenant mismatch: command tenant=" + command.tenantId()
                    + ", record tenant=" + result.record().tenantId()));
        }
        return Uni.createFrom().item(result);
    }

    interface Effects {
        Uni<Boolean> itemContinuationReady(AwaitInteractionRecord record, AwaitUnitRecord unit);

        void dispatchItemContinuation(
            AwaitInteractionRecord record,
            AwaitUnitRecord unit,
            AwaitItemContinuationHandler itemContinuationHandler,
            long nowEpochMs);

        Uni<Void> releaseAwaitResume(AwaitInteractionRecord record, String awaitUnitId, long nowEpochMs);

        void recordEarlyCompletionHeld(AwaitInteractionRecord record, AwaitUnitRecord unit);
    }
}
