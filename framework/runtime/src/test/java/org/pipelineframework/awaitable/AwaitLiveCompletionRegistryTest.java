package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitLiveCompletionRegistryTest {

    @Test
    void releasesOnePermitOnlyAfterTheMatchingCompletionIsAccepted() {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        AwaitLiveCompletionRegistry.LiveAwaitSession<String> session = registry.open(descriptor(), "tenant", "unit");
        AssertSubscriber<String> subscriber = AssertSubscriber.create(1);
        Multi.createFrom().publisher(session).subscribe().withSubscriber(subscriber);

        session.acquirePermit("item:0", 1).await().indefinitely();
        CompletableFuture<Void> second = session.acquirePermit("item:1", 1).subscribeAsCompletionStage();
        assertFalse(second.isDone());

        session.accept(completion(0)).await().indefinitely();
        subscriber.awaitItems(1, Duration.ofSeconds(5));
        assertTrue(second.isDone());

        session.accept(completion(0)).await().indefinitely();
        CompletableFuture<Void> third = session.acquirePermit("item:2", 1).subscribeAsCompletionStage();
        assertFalse(third.isDone());

        subscriber.request(1);
        session.accept(completion(1)).await().indefinitely();
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        assertTrue(third.isDone());
    }

    @Test
    void terminalFailureRejectsQueuedPermitsAndStopsFurtherDispatch() {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        AwaitLiveCompletionRegistry.LiveAwaitSession<String> session = registry.open(descriptor(), "tenant", "unit");

        session.acquirePermit("item:0", 1).await().indefinitely();
        CompletableFuture<Void> queued = session.acquirePermit("item:1", 1).subscribeAsCompletionStage();

        session.fail(new IllegalStateException("provider timed out"));

        assertTrue(queued.isCompletedExceptionally());
        assertThrows(IllegalStateException.class,
            () -> session.acquirePermit("item:2", 1).await().indefinitely());
    }

    private static AwaitStepDescriptor descriptor() {
        return new AwaitStepDescriptor(
            "review",
            String.class.getName(),
            String.class.getName(),
            "ONE_TO_ONE",
            Duration.ofMinutes(5),
            "interactionId",
            "kafka",
            Map.of(),
            java.util.List.of());
    }

    private static AwaitInteractionRecord completion(int index) {
        long now = System.currentTimeMillis();
        return new AwaitInteractionRecord(
            "tenant", "execution", "review", 1, String.class.getName(),
            "interaction-" + index, "correlation-" + index, "causation-" + index, "idempotency-" + index,
            0L, AwaitInteractionStatus.COMPLETED,
            "request-" + index, "response-" + index, "unit", index, null,
            null, null, "kafka", Map.of(), now + 300000, now, now, now + 86400);
    }
}
