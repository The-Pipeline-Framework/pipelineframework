package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class LiveAwaitSessionTest {

    @Test
    void failedPayloadCoercionDoesNotMarkCompletionAccepted() {
        AtomicInteger closed = new AtomicInteger();
        LiveAwaitSession<Integer> session = new LiveAwaitSession<>("unit-1", Integer.class, closed::incrementAndGet);
        RecordingSubscriber<Integer> subscriber = new RecordingSubscriber<>();
        session.subscribe(subscriber);
        subscriber.subscription().request(1);
        AwaitInteractionRecord record = itemRecord("unit-1", 0, Map.of("not", "an-integer"));

        assertThrows(IllegalStateException.class, () -> session.accept(record).await().atMost(Duration.ofSeconds(5)));
        assertThrows(IllegalStateException.class, () -> session.awaitAccepted(record).await().atMost(Duration.ofSeconds(5)));

        assertTrue(subscriber.items().isEmpty());
        assertTrue(subscriber.failure() instanceof IllegalStateException);
        assertEquals(1, closed.get());
    }

    private static AwaitInteractionRecord itemRecord(String unitId, int index, Object responsePayload) {
        long now = System.currentTimeMillis();
        return new AwaitInteractionRecord(
            "tenant1",
            "exec123",
            "review",
            2,
            Integer.class.getName(),
            "interaction-" + index,
            "correlation-" + index,
            "causation-" + index,
            "idem-" + index,
            0L,
            AwaitInteractionStatus.COMPLETED,
            "request-" + index,
            responsePayload,
            unitId,
            index,
            null,
            null,
            null,
            "kafka",
            Map.of(),
            now + 300000,
            now,
            now,
            now + 86400);
    }

    private static final class RecordingSubscriber<T> implements Flow.Subscriber<T> {
        private final List<T> items = new ArrayList<>();
        private Flow.Subscription subscription;
        private Throwable failure;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(T item) {
            items.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            failure = throwable;
        }

        @Override
        public void onComplete() {
        }

        private Flow.Subscription subscription() {
            return subscription;
        }

        private List<T> items() {
            return items;
        }

        private Throwable failure() {
            return failure;
        }
    }
}
