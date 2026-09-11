package org.pipelineframework.awaitable.store;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pipelineframework.awaitable.*;
import static org.junit.jupiter.api.Assertions.*;

class CommandCompletionGateTest {
    private final InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
    private final long now = System.currentTimeMillis();

    @ParameterizedTest
    @EnumSource(CommandDispatchSettlement.class)
    void racingCallbackIsStoredWithoutCompletionUntilOutcomeSettles(CommandDispatchSettlement outcome) {
        AwaitInteractionRecord dispatching = claim(create());
        var observed = complete(dispatching, "result");
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, observed.record().status());
        assertFalse(observed.record().status().terminal());
        assertEquals("result", observed.record().responsePayload());
        assertTrue(complete(dispatching, "different duplicate").duplicate());
        assertTrue(store.settleCommandDispatch(dispatching, outcome, now + 1).await().indefinitely().isEmpty());
        var settled = store.settleCommandDispatch(observed.record(), outcome, now + 1).await().indefinitely().orElseThrow();
        boolean accepted = outcome == CommandDispatchSettlement.SUCCEEDED || outcome == CommandDispatchSettlement.AMBIGUOUS;
        assertEquals(accepted ? AwaitInteractionStatus.COMPLETED : AwaitInteractionStatus.FAILED, settled.status());
        assertEquals("result", settled.responsePayload());
        if (!accepted) {
            assertEquals("contradictory-provider-evidence", settled.transportMetadata().get("completionFailure"));
        }
        assertTrue(store.markDispatching(settled.tenantId(), settled.interactionId(), settled.version(), now + 2)
            .await().indefinitely().isEmpty());
        assertEquals(settled.interactionId(), create().interactionId());
    }

    @ParameterizedTest
    @EnumSource(value = CommandDispatchSettlement.class, names = {"SUCCEEDED", "AMBIGUOUS"})
    void callbackAfterSettledDispatchCompletesNormally(CommandDispatchSettlement outcome) {
        var dispatching = claim(create());
        var dispatched = store.settleCommandDispatch(dispatching, outcome, now + 1).await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.DISPATCHED, dispatched.status());
        assertEquals(AwaitInteractionStatus.COMPLETED, complete(dispatched, "result").record().status());
    }

    @Test
    void definiteRetryRetainsIdentityDeadlineAndRejectsCallbackBeforeNextDispatch() {
        var dispatching = claim(create());
        var retry = store.markDispatchRetryable(dispatching, now + 1).await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.WAITING, retry.status());
        assertEquals(dispatching.deadlineEpochMs(), retry.deadlineEpochMs());
        assertEquals(retry.interactionId(), create().interactionId());
        assertThrows(IllegalStateException.class, () -> complete(retry, "too early"));
        var redispatched = claim(retry);
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, complete(redispatched, "result").record().status());
    }

    @Test
    void timeoutAndCancellationWinAgainstSettlement() {
        var observed = complete(claim(create()), "result").record();
        var cancelled = store.cancel(observed.tenantId(), observed.interactionId(), observed.version(), "cancelled", now + 1)
            .await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.CANCELLED, cancelled.status());
        assertTrue(store.settleCommandDispatch(observed, CommandDispatchSettlement.SUCCEEDED, now + 2)
            .await().indefinitely().isEmpty());
        assertEquals(AwaitInteractionStatus.TIMED_OUT,
            observed.settleCommandDispatch(CommandDispatchSettlement.SUCCEEDED, observed.deadlineEpochMs()).status());
    }

    private AwaitInteractionRecord create() {
        return store.createOrGet(new AwaitCreateCommand("tenant", "execution", "start", 0,
            String.class.getName(), String.class.getName(), "cause", "stable", "correlation", "input", "", "", "",
            "unit", 0, Map.of("completionMode", "CONNECTOR_CALLBACK"), now, now + 60_000, now / 1000 + 86_400))
            .await().indefinitely().record();
    }

    private AwaitInteractionRecord claim(AwaitInteractionRecord record) {
        return store.markDispatching(record.tenantId(), record.interactionId(), record.version(), record.transportMetadata(), now)
            .await().indefinitely().orElseThrow();
    }

    private AwaitCompletionResult complete(AwaitInteractionRecord record, String response) {
        return store.complete(new AwaitCompletionCommand(record.tenantId(), record.interactionId(), record.correlationId(),
            "token", "completion", response, "provider", now)).await().indefinitely();
    }
}
