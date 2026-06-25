package org.pipelineframework.awaitable;

/**
 * Effect emitted by the immutable live-await session state machine.
 *
 * <p>The session wrapper interprets these effects against Flow subscribers and
 * completion waiters; the state machine itself only decides what should happen.
 */
sealed interface LiveAwaitSessionEffect<O>
    permits LiveAwaitSessionEffect.EmitItem,
    LiveAwaitSessionEffect.CompleteStream,
    LiveAwaitSessionEffect.FailStream,
    LiveAwaitSessionEffect.FailAcceptance,
    LiveAwaitSessionEffect.FailAcceptedWaiters,
    LiveAwaitSessionEffect.CloseSession {

    record EmitItem<O>(String completionKey, O item) implements LiveAwaitSessionEffect<O> {
    }

    record CompleteStream<O>() implements LiveAwaitSessionEffect<O> {
    }

    record FailStream<O>(Throwable failure) implements LiveAwaitSessionEffect<O> {
    }

    record FailAcceptance<O>(String completionKey, Throwable failure) implements LiveAwaitSessionEffect<O> {
    }

    record FailAcceptedWaiters<O>(Throwable failure) implements LiveAwaitSessionEffect<O> {
    }

    record CloseSession<O>() implements LiveAwaitSessionEffect<O> {
    }
}
