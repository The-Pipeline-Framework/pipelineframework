package org.pipelineframework.awaitable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

record LiveAwaitSessionState<O>(
    boolean subscriberAttached,
    boolean subscriberReady,
    long requested,
    List<Pending<O>> pending,
    Set<String> seenCompletions,
    Set<String> acceptedCompletions,
    boolean dispatchComplete,
    int expectedItemCount,
    int emittedItemCount,
    boolean cancelled,
    boolean terminated,
    boolean terminalSignalDelivered,
    Throwable terminalFailure) {

    LiveAwaitSessionState {
        pending = List.copyOf(Objects.requireNonNull(pending, "pending must not be null"));
        seenCompletions = Set.copyOf(Objects.requireNonNull(seenCompletions, "seenCompletions must not be null"));
        acceptedCompletions = Set.copyOf(
            Objects.requireNonNull(acceptedCompletions, "acceptedCompletions must not be null"));
    }

    static <O> LiveAwaitSessionState<O> initial() {
        return new LiveAwaitSessionState<>(
            false,
            false,
            0L,
            List.of(),
            Set.of(),
            Set.of(),
            false,
            0,
            0,
            false,
            false,
            false,
            null);
    }

    boolean duplicate(String completionKey) {
        return seenCompletions.contains(completionKey);
    }

    boolean accepted(String completionKey) {
        return acceptedCompletions.contains(completionKey);
    }

    boolean canAcceptCompletion() {
        return !cancelled && !terminated;
    }

    LiveAwaitSessionDecision<O> attachSubscriber() {
        if (subscriberAttached) {
            return LiveAwaitSessionDecision.unchanged(this);
        }
        return LiveAwaitSessionDecision.unchanged(withSubscriberAttached(true));
    }

    LiveAwaitSessionDecision<O> markSubscriberReady() {
        return withSubscriberReady(true).drain();
    }

    LiveAwaitSessionDecision<O> admitCompletion(String completionKey, O item) {
        if (!canAcceptCompletion() || duplicate(completionKey)) {
            return LiveAwaitSessionDecision.unchanged(this);
        }
        List<Pending<O>> nextPending = new ArrayList<>(pending);
        nextPending.add(new Pending<>(completionKey, item));
        Set<String> nextSeen = new HashSet<>(seenCompletions);
        nextSeen.add(completionKey);
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            nextPending,
            nextSeen,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure).drain();
    }

    LiveAwaitSessionDecision<O> markDispatchComplete(int expectedItemCount) {
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            pending,
            seenCompletions,
            acceptedCompletions,
            true,
            Math.max(0, expectedItemCount),
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure).drain();
    }

    LiveAwaitSessionDecision<O> request(long n) {
        if (n <= 0) {
            return fail(new IllegalArgumentException("request amount must be positive"));
        }
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            addCap(requested, n),
            pending,
            seenCompletions,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure).drain();
    }

    LiveAwaitSessionDecision<O> cancel(String unitId) {
        if (terminated) {
            return LiveAwaitSessionDecision.unchanged(this);
        }
        IllegalStateException failure = new IllegalStateException(
            "Live await stream cancelled for unit " + unitId);
        List<LiveAwaitSessionEffect<O>> effects = failPendingEffects(failure);
        effects.add(new LiveAwaitSessionEffect.FailAcceptedWaiters<>(failure));
        effects.add(new LiveAwaitSessionEffect.CloseSession<>());
        return LiveAwaitSessionDecision.of(new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            List.of(),
            seenCompletions,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            true,
            true,
            terminalSignalDelivered,
            terminalFailure), effects);
    }

    LiveAwaitSessionDecision<O> fail(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        if (terminated) {
            return LiveAwaitSessionDecision.unchanged(this);
        }
        List<LiveAwaitSessionEffect<O>> effects = failPendingEffects(failure);
        effects.add(new LiveAwaitSessionEffect.FailAcceptedWaiters<>(failure));
        LiveAwaitSessionState<O> failed = new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            List.of(),
            seenCompletions,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            true,
            terminalSignalDelivered,
            failure);
        LiveAwaitSessionDecision<O> decision = append(failed.drain(), effects);
        List<LiveAwaitSessionEffect<O>> effectsWithClose = new ArrayList<>(decision.effects());
        if (effectsWithClose.stream().noneMatch(LiveAwaitSessionEffect.CloseSession.class::isInstance)) {
            effectsWithClose.add(new LiveAwaitSessionEffect.CloseSession<>());
        }
        return LiveAwaitSessionDecision.of(decision.state(), effectsWithClose);
    }

    LiveAwaitSessionDecision<O> acceptanceCompleted(String completionKey) {
        Set<String> nextAccepted = new HashSet<>(acceptedCompletions);
        nextAccepted.add(completionKey);
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            pending,
            seenCompletions,
            nextAccepted,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure).drain();
    }

    LiveAwaitSessionDecision<O> drain() {
        LiveAwaitSessionState<O> current = this;
        List<LiveAwaitSessionEffect<O>> effects = new ArrayList<>();
        while (!current.terminated
            && current.subscriberReady
            && current.subscriberAttached
            && current.requested > 0
            && !current.pending.isEmpty()) {
            ArrayDeque<Pending<O>> queue = new ArrayDeque<>(current.pending);
            Pending<O> next = queue.removeFirst();
            long nextRequested = current.requested == Long.MAX_VALUE ? Long.MAX_VALUE : current.requested - 1;
            current = new LiveAwaitSessionState<>(
                current.subscriberAttached,
                current.subscriberReady,
                nextRequested,
                List.copyOf(queue),
                current.seenCompletions,
                current.acceptedCompletions,
                current.dispatchComplete,
                current.expectedItemCount,
                current.emittedItemCount + 1,
                current.cancelled,
                current.terminated,
                current.terminalSignalDelivered,
                current.terminalFailure);
            effects.add(new LiveAwaitSessionEffect.EmitItem<>(next.completionKey(), next.item()));
        }
        if (!current.terminalSignalDelivered
            && current.terminated
            && current.terminalFailure != null
            && current.subscriberReady
            && current.subscriberAttached) {
            current = current.withTerminalSignalDelivered(true);
            effects.add(new LiveAwaitSessionEffect.FailStream<>(current.terminalFailure));
            effects.add(new LiveAwaitSessionEffect.CloseSession<>());
        } else if (!current.terminated
            && current.subscriberAttached
            && current.subscriberReady
            && current.dispatchComplete
            && current.pending.isEmpty()
            && current.emittedItemCount >= current.expectedItemCount) {
            current = current.withTerminated(true).withTerminalSignalDelivered(true);
            effects.add(new LiveAwaitSessionEffect.CompleteStream<>());
            effects.add(new LiveAwaitSessionEffect.CloseSession<>());
        }
        return LiveAwaitSessionDecision.of(current, effects);
    }

    private List<LiveAwaitSessionEffect<O>> failPendingEffects(Throwable failure) {
        List<LiveAwaitSessionEffect<O>> effects = new ArrayList<>();
        for (Pending<O> item : pending) {
            effects.add(new LiveAwaitSessionEffect.FailAcceptance<>(item.completionKey(), failure));
        }
        return effects;
    }

    private LiveAwaitSessionState<O> withSubscriberAttached(boolean subscriberAttached) {
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            pending,
            seenCompletions,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure);
    }

    private LiveAwaitSessionState<O> withSubscriberReady(boolean subscriberReady) {
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            pending,
            seenCompletions,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure);
    }

    private LiveAwaitSessionState<O> withTerminated(boolean terminated) {
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            pending,
            seenCompletions,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure);
    }

    private LiveAwaitSessionState<O> withTerminalSignalDelivered(boolean terminalSignalDelivered) {
        return new LiveAwaitSessionState<>(
            subscriberAttached,
            subscriberReady,
            requested,
            pending,
            seenCompletions,
            acceptedCompletions,
            dispatchComplete,
            expectedItemCount,
            emittedItemCount,
            cancelled,
            terminated,
            terminalSignalDelivered,
            terminalFailure);
    }

    private static <O> LiveAwaitSessionDecision<O> append(
        LiveAwaitSessionDecision<O> decision,
        List<LiveAwaitSessionEffect<O>> before) {
        List<LiveAwaitSessionEffect<O>> effects = new ArrayList<>(before);
        effects.addAll(decision.effects());
        return LiveAwaitSessionDecision.of(decision.state(), effects);
    }

    private static long addCap(long current, long increment) {
        long updated = current + increment;
        return updated < 0 ? Long.MAX_VALUE : updated;
    }

    record Pending<O>(String completionKey, O item) {
    }
}
