package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void enqueuesCompletionBeforeDownstreamDemandWhileRetainingTheLocalPermit() {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        AwaitLiveCompletionRegistry.LiveAwaitSession<String> session = registry.open(descriptor(), "tenant", "unit");
        AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
        Multi.createFrom().publisher(session).subscribe().withSubscriber(subscriber);

        session.acquirePermit("item:0", 1).await().indefinitely();
        CompletableFuture<Void> next = session.acquirePermit("item:1", 1).subscribeAsCompletionStage();

        session.enqueue(completion(0)).await().indefinitely();
        assertFalse(next.isDone());

        subscriber.request(1);
        subscriber.awaitItems(1, Duration.ofSeconds(5));
        assertTrue(next.isDone());
    }

    @Test
    void signalReportsOnlyNewlyEnqueuedCompletions() {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        registry.open(descriptor(), "tenant", "unit");

        assertTrue(registry.signal(completion(0)).await().indefinitely());
        assertFalse(registry.signal(completion(0)).await().indefinitely());
    }

    @Test
    void enqueuesAFullBurstWithoutReleasingLocalPermitsBeforeDelivery() {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        AwaitLiveCompletionRegistry.LiveAwaitSession<String> session = registry.open(descriptor(), "tenant", "unit");
        AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
        Multi.createFrom().publisher(session).subscribe().withSubscriber(subscriber);

        for (int index = 0; index < 3; index++) {
            session.acquirePermit("item:" + index, 3).await().indefinitely();
        }
        CompletableFuture<Void> next = session.acquirePermit("item:3", 3).subscribeAsCompletionStage();

        for (int index = 0; index < 3; index++) {
            session.enqueue(completion(index)).await().indefinitely();
        }
        session.enqueue(completion(0)).await().indefinitely();
        assertFalse(next.isDone());

        subscriber.request(3);
        subscriber.awaitItems(3, Duration.ofSeconds(5));
        assertTrue(next.isDone());
    }

    @Test
    void enqueuesBeforeBlockingEagerDownstreamDelivery() throws Exception {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        AwaitLiveCompletionRegistry.LiveAwaitSession<String> session = registry.open(descriptor(), "tenant", "unit");
        CountDownLatch delivered = new CountDownLatch(1);
        Multi.createFrom().publisher(session).subscribe().withSubscriber(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(String item) {
                delivered.countDown();
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onError(Throwable failure) {
            }

            @Override
            public void onComplete() {
            }
        });

        long started = System.nanoTime();
        session.enqueue(completion(0)).await().indefinitely();

        assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofMillis(500)) < 0);
        assertTrue(delivered.await(2, TimeUnit.SECONDS));
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

    @Test
    void deliversTheCanonicalCompletionWithoutApplyingTheTransportAdapterAgain() {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "v3", CanonicalStatus.class.getName(), CanonicalStatus.class.getName(), "ONE_TO_ONE",
            Duration.ofMinutes(5), "interactionId", "kafka", Map.of(), java.util.List.of(),
            String.class.getName(), TransportStatus.class.getName(),
            java.util.function.Function.identity(), ignored -> {
                throw new AssertionError("the live registry must not convert an already canonical completion");
            });
        AwaitLiveCompletionRegistry.LiveAwaitSession<CanonicalStatus> session = registry.open(descriptor, "tenant", "unit");
        AssertSubscriber<CanonicalStatus> subscriber = AssertSubscriber.create(1);
        Multi.createFrom().publisher(session).subscribe().withSubscriber(subscriber);

        CanonicalStatus expected = new CanonicalStatus("approved");
        session.enqueue(canonicalCompletion(expected)).await().indefinitely();

        subscriber.awaitItems(1, Duration.ofSeconds(5));
        assertEquals(expected, subscriber.getItems().getFirst());
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

    private static AwaitInteractionRecord canonicalCompletion(CanonicalStatus value) {
        long now = System.currentTimeMillis();
        return new AwaitInteractionRecord(
            "tenant", "execution", "v3", 1, CanonicalStatus.class.getName(),
            "interaction-v3", "correlation-v3", "causation-v3", "idempotency-v3",
            0L, AwaitInteractionStatus.COMPLETED,
            "request", value, "unit", 0, null,
            null, null, "kafka", Map.of(), now + 300000, now, now, now + 86400);
    }

    private record CanonicalStatus(String value) {
    }

    private record TransportStatus(String value) {
    }
}
