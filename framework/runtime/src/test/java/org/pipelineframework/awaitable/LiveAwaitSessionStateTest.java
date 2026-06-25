package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LiveAwaitSessionStateTest {

    @Test
    void zeroItemDispatchCompletesAfterSubscriberIsReady() {
        LiveAwaitSessionState<String> state = attachedReadyState();

        LiveAwaitSessionDecision<String> decision = state.markDispatchComplete(0);

        assertTrue(decision.state().terminated());
        assertEquals(2, decision.effects().size());
        assertInstanceOf(LiveAwaitSessionEffect.CompleteStream.class, decision.effects().get(0));
        assertInstanceOf(LiveAwaitSessionEffect.CloseSession.class, decision.effects().get(1));
    }

    @Test
    void completionBeforeSubscriberReadinessDoesNotEmitTerminalSignalsEarly() {
        LiveAwaitSessionState<String> state = LiveAwaitSessionState.<String>initial()
            .attachSubscriber()
            .state()
            .request(1)
            .state();

        LiveAwaitSessionDecision<String> dispatchComplete = state.markDispatchComplete(1);
        assertTrue(dispatchComplete.effects().isEmpty());

        LiveAwaitSessionDecision<String> admitted = dispatchComplete.state().admitCompletion("item:0", "approved");
        assertTrue(admitted.effects().isEmpty());
        assertFalse(admitted.state().terminated());

        LiveAwaitSessionDecision<String> ready = admitted.state().markSubscriberReady();
        assertEquals(3, ready.effects().size());
        assertInstanceOf(LiveAwaitSessionEffect.EmitItem.class, ready.effects().get(0));
        assertInstanceOf(LiveAwaitSessionEffect.CompleteStream.class, ready.effects().get(1));
        assertInstanceOf(LiveAwaitSessionEffect.CloseSession.class, ready.effects().get(2));
    }

    @Test
    void duplicateCompletionsAreIdempotent() {
        LiveAwaitSessionState<String> state = attachedReadyState().request(1).state();

        LiveAwaitSessionDecision<String> first = state.admitCompletion("item:0", "approved");
        assertEquals(1, first.effects().size());
        assertInstanceOf(LiveAwaitSessionEffect.EmitItem.class, first.effects().getFirst());

        LiveAwaitSessionDecision<String> accepted = first.state().acceptanceCompleted("item:0");
        LiveAwaitSessionDecision<String> duplicate = accepted.state().admitCompletion("item:0", "duplicate");

        assertTrue(duplicate.effects().isEmpty());
        assertTrue(duplicate.state().duplicate("item:0"));
        assertTrue(duplicate.state().accepted("item:0"));
    }

    @Test
    void demandControlsEmission() {
        LiveAwaitSessionState<String> state = attachedReadyState();

        state = state.admitCompletion("item:0", "first").state();
        state = state.admitCompletion("item:1", "second").state();
        assertEquals(2, state.pending().size());

        LiveAwaitSessionDecision<String> firstDemand = state.request(1);
        assertEquals(1, firstDemand.effects().size());
        LiveAwaitSessionEffect.EmitItem<?> firstEmit =
            assertInstanceOf(LiveAwaitSessionEffect.EmitItem.class, firstDemand.effects().getFirst());
        assertEquals("item:0", firstEmit.completionKey());
        assertEquals(1, firstDemand.state().pending().size());

        LiveAwaitSessionDecision<String> secondDemand = firstDemand.state().request(1);
        assertEquals(1, secondDemand.effects().size());
        LiveAwaitSessionEffect.EmitItem<?> secondEmit =
            assertInstanceOf(LiveAwaitSessionEffect.EmitItem.class, secondDemand.effects().getFirst());
        assertEquals("item:1", secondEmit.completionKey());
        assertTrue(secondDemand.state().pending().isEmpty());
    }

    private static LiveAwaitSessionState<String> attachedReadyState() {
        return LiveAwaitSessionState.<String>initial()
            .attachSubscriber()
            .state()
            .markSubscriberReady()
            .state();
    }
}
