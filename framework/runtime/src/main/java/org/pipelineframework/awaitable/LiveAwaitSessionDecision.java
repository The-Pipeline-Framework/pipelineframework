package org.pipelineframework.awaitable;

import java.util.List;
import java.util.Objects;

record LiveAwaitSessionDecision<O>(
    LiveAwaitSessionState<O> state,
    List<LiveAwaitSessionEffect<O>> effects) {

    LiveAwaitSessionDecision {
        Objects.requireNonNull(state, "state must not be null");
        effects = List.copyOf(Objects.requireNonNull(effects, "effects must not be null"));
    }

    static <O> LiveAwaitSessionDecision<O> of(
        LiveAwaitSessionState<O> state,
        List<LiveAwaitSessionEffect<O>> effects) {
        return new LiveAwaitSessionDecision<>(state, effects);
    }

    static <O> LiveAwaitSessionDecision<O> unchanged(LiveAwaitSessionState<O> state) {
        return new LiveAwaitSessionDecision<>(state, List.of());
    }
}
