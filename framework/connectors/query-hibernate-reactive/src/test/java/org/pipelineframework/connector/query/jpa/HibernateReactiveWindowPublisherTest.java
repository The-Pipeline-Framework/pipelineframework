package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.UniEmitter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.jupiter.api.Test;

class HibernateReactiveWindowPublisherTest {
    @Test
    void fetchesOnlyDemandedBoundedWindowsInOrder() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        List<String> source = List.of("a", "b", "c");
        List<String> calls = new ArrayList<>();
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> {
                calls.add(offset + ":" + size);
                int end = Math.min(offset + size, source.size());
                return Uni.createFrom().item(offset >= source.size() ? List.of() : source.subList(offset, end));
            },
            2,
            Optional.empty());
        AssertSubscriber<String> subscriber = Multi.createFrom().publisher(publisher)
            .subscribe().withSubscriber(AssertSubscriber.create(0));

        assertTrue(calls.isEmpty());
        subscriber.request(1).awaitItems(1).assertItems("a");
        assertEquals(List.of("0:1"), calls);
        subscriber.request(2).awaitItems(3).assertItems("a", "b", "c");
        assertEquals(List.of("0:1", "1:2"), calls);
        assertFalse(publisher.termination().toCompletableFuture().isDone());
        subscriber.request(1).awaitCompletion(Duration.ofSeconds(5)).assertCompleted();
        assertEquals(List.of("0:1", "1:2", "3:1"), calls);
        publisher.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(closed.get());
    }

    @Test
    void permitsOnlyOneInFlightWindowAndHonoursSemanticLimit() {
        AtomicBoolean closed = new AtomicBoolean();
        List<String> calls = new ArrayList<>();
        @SuppressWarnings("unchecked")
        UniEmitter<? super List<String>>[] pending = new UniEmitter[1];
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> {
                calls.add(offset + ":" + size);
                return Uni.createFrom().emitter(emitter -> pending[0] = emitter);
            },
            4,
            Optional.of(2));
        AssertSubscriber<String> subscriber = Multi.createFrom().publisher(publisher)
            .subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.request(1);
        subscriber.request(10);
        assertEquals(List.of("0:1"), calls);
        pending[0].complete(List.of("a"));
        subscriber.awaitItems(1);
        assertEquals(List.of("0:1", "1:1"), calls);
        pending[0].complete(List.of("b"));

        subscriber.awaitCompletion(Duration.ofSeconds(5)).assertItems("a", "b").assertCompleted();
        assertTrue(closed.get());
    }

    @Test
    void cancellationStopsFurtherWindowsAndFinishesSessionLifetime() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        List<String> calls = new ArrayList<>();
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> {
                calls.add(offset + ":" + size);
                return Uni.createFrom().item(List.of("a"));
            },
            4,
            Optional.empty());
        AssertSubscriber<String> subscriber = Multi.createFrom().publisher(publisher)
            .subscribe().withSubscriber(AssertSubscriber.create(1));

        subscriber.awaitItems(1).cancel();
        publisher.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(List.of("0:1"), calls);
        assertTrue(closed.get());
    }

    @Test
    void queryFailureClosesTheSessionAndFailsTheStream() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> Uni.createFrom().failure(new IllegalStateException("page failed")),
            4,
            Optional.empty());
        AssertSubscriber<String> subscriber = Multi.createFrom().publisher(publisher)
            .subscribe().withSubscriber(AssertSubscriber.create(1));

        subscriber.awaitFailure(Duration.ofSeconds(5)).assertFailedWith(IllegalStateException.class, "page failed");
        publisher.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(closed.get());
    }

    @Test
    void synchronousQueryFailureClosesTheSessionAndFailsTheStream() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> {
                throw new IllegalStateException("query construction failed");
            },
            4,
            Optional.empty());
        AssertSubscriber<String> subscriber = Multi.createFrom().publisher(publisher)
            .subscribe().withSubscriber(AssertSubscriber.create(1));

        subscriber.awaitFailure(Duration.ofSeconds(5))
            .assertFailedWith(IllegalStateException.class, "query construction failed");
        publisher.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(closed.get());
    }

    @Test
    void reentrantDemandAdvancesTheOffsetBeforeOpeningAnotherWindow() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        List<String> source = List.of("a", "b", "c");
        List<Integer> offsets = new ArrayList<>();
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> {
                offsets.add(offset);
                int end = Math.min(offset + size, source.size());
                return Uni.createFrom().item(offset >= source.size() ? List.of() : source.subList(offset, end));
            },
            2,
            Optional.empty());
        List<String> rows = new ArrayList<>();
        CompletableFuture<Void> completed = new CompletableFuture<>();

        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription = value;
                value.request(1);
            }

            @Override
            public void onNext(String item) {
                rows.add(item);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable failure) {
                completed.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
                completed.complete(null);
            }
        });

        completed.get(5, TimeUnit.SECONDS);
        publisher.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(List.of("a", "b", "c"), rows);
        assertEquals(List.of(0, 1, 2, 3), offsets);
        assertTrue(closed.get());
    }

    @Test
    void concurrentInvalidDemandCannotSignalErrorBeforeAnInFlightRow() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        @SuppressWarnings("unchecked")
        UniEmitter<? super List<String>>[] pending = new UniEmitter[1];
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> Uni.createFrom().emitter(emitter -> pending[0] = emitter),
            2,
            Optional.empty());
        CompletableFuture<Flow.Subscription> subscription = new CompletableFuture<>();
        CountDownLatch rowStarted = new CountDownLatch(1);
        CountDownLatch releaseRow = new CountDownLatch(1);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        List<String> signals = new CopyOnWriteArrayList<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription.complete(value);
                value.request(1);
            }

            @Override
            public void onNext(String item) {
                signals.add("next:" + item);
                rowStarted.countDown();
                try {
                    assertTrue(releaseRow.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            }

            @Override
            public void onError(Throwable failure) {
                signals.add("error:" + failure.getMessage());
                failed.complete(null);
            }

            @Override
            public void onComplete() {
                signals.add("complete");
            }
        });

        CompletableFuture<Void> delivery = CompletableFuture.runAsync(() -> pending[0].complete(List.of("row")));
        assertTrue(rowStarted.await(5, TimeUnit.SECONDS));
        CompletableFuture<Void> invalidDemand = CompletableFuture.runAsync(() ->
            subscription.join().request(0));
        assertFalse(failed.isDone());
        releaseRow.countDown();

        delivery.get(5, TimeUnit.SECONDS);
        invalidDemand.get(5, TimeUnit.SECONDS);
        failed.get(5, TimeUnit.SECONDS);
        publisher.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(List.of("next:row", "error:stream demand must be positive"), signals);
        assertTrue(closed.get());
    }

    @Test
    void reentrantInvalidDemandDefersErrorUntilOnNextReturns() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        HibernateReactiveWindowPublisher<String> publisher = new HibernateReactiveWindowPublisher<>(
            sessionFactory(closed),
            (session, offset, size) -> Uni.createFrom().item(List.of("row")),
            2,
            Optional.empty());
        List<String> signals = new ArrayList<>();
        CompletableFuture<Void> failed = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription = value;
                value.request(1);
            }

            @Override
            public void onNext(String item) {
                signals.add("next:" + item);
                subscription.request(0);
                signals.add("onNext-returning");
            }

            @Override
            public void onError(Throwable failure) {
                signals.add("error:" + failure.getMessage());
                failed.complete(null);
            }

            @Override
            public void onComplete() {
                signals.add("complete");
            }
        });

        failed.get(5, TimeUnit.SECONDS);
        publisher.termination().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(List.of(
            "next:row", "onNext-returning", "error:stream demand must be positive"), signals);
        assertTrue(closed.get());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Mutiny.SessionFactory sessionFactory(AtomicBoolean closed) {
        Mutiny.SessionFactory factory = mock(Mutiny.SessionFactory.class);
        Mutiny.Session session = mock(Mutiny.Session.class);
        when(factory.withSession(any())).thenAnswer(invocation -> {
            Function<Mutiny.Session, Uni<Object>> work = invocation.getArgument(0);
            return work.apply(session).eventually(() -> closed.set(true));
        });
        return factory;
    }
}
