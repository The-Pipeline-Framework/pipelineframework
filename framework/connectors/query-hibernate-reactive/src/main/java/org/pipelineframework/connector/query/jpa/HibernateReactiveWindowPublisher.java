package org.pipelineframework.connector.query.jpa;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hibernate.reactive.mutiny.Mutiny;

/** Adapts bounded Hibernate Reactive result windows to a demand-aware row publisher. */
final class HibernateReactiveWindowPublisher<T> implements Flow.Publisher<T> {
    private final Mutiny.SessionFactory sessionFactory;
    private final PageFetcher<T> pageFetcher;
    private final int maximumWindow;
    private final Optional<Integer> semanticLimit;
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final CompletableFuture<Void> providerDone = new CompletableFuture<>();
    private final AtomicBoolean subscribed = new AtomicBoolean();

    HibernateReactiveWindowPublisher(
        Mutiny.SessionFactory sessionFactory,
        PageFetcher<T> pageFetcher,
        int maximumWindow,
        Optional<Integer> semanticLimit
    ) {
        this.sessionFactory = Objects.requireNonNull(sessionFactory, "sessionFactory must not be null");
        this.pageFetcher = Objects.requireNonNull(pageFetcher, "pageFetcher must not be null");
        if (maximumWindow <= 0) {
            throw new IllegalArgumentException("maximumWindow must be positive");
        }
        this.maximumWindow = maximumWindow;
        this.semanticLimit = Objects.requireNonNull(semanticLimit, "semanticLimit must not be null");
    }

    CompletionStage<Void> termination() {
        return termination;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber must not be null");
        if (!subscribed.compareAndSet(false, true)) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.onError(new IllegalStateException("Hibernate Reactive Query stream supports one subscription"));
            return;
        }
        subscriber.onSubscribe(new WindowSubscription(subscriber));
    }

    @FunctionalInterface
    interface PageFetcher<T> {
        Uni<List<T>> fetch(Mutiny.Session session, int offset, int size);
    }

    private final class WindowSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super T> downstream;
        private final AtomicLong requested = new AtomicLong();
        private final Object signalLock = new Object();
        private Mutiny.Session session;
        private Cancellable active;
        private int offset;
        private boolean inFlight;
        private volatile boolean cancelled;
        private volatile boolean publisherTerminated;
        private boolean lifecycleStarted;
        private boolean emitting;
        private Throwable pendingFailure;
        private boolean pendingCompletion;

        private WindowSubscription(Flow.Subscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void request(long count) {
            if (count <= 0) {
                fail(new IllegalArgumentException("stream demand must be positive"));
                return;
            }
            addRequested(count);
            drain();
        }

        @Override
        public void cancel() {
            Cancellable running;
            synchronized (this) {
                if (cancelled || publisherTerminated) {
                    return;
                }
                cancelled = true;
                running = active;
            }
            if (running != null) {
                running.cancel();
            }
            synchronized (signalLock) {
                // Wait for an already-started downstream signal before cancellation returns.
            }
            finishResources();
        }

        private void drain() {
            Mutiny.Session resolvedSession;
            int window;
            synchronized (this) {
                if (cancelled || publisherTerminated || inFlight || requested.get() == 0) {
                    return;
                }
                inFlight = true;
                resolvedSession = session;
                window = resolvedSession == null ? 0 : nextWindow();
            }
            if (resolvedSession == null) {
                startSessionLifecycle();
                return;
            }
            if (window == 0) {
                complete();
                return;
            }
            AtomicBoolean callbackCompleted = new AtomicBoolean();
            Cancellable started;
            try {
                Uni<List<T>> page = Objects.requireNonNull(
                    pageFetcher.fetch(resolvedSession, offset, window),
                    "Hibernate Reactive Query page Uni must not be null");
                started = page.subscribe().with(rows -> {
                    callbackCompleted.set(true);
                    pageReceived(rows, window);
                }, failure -> {
                    callbackCompleted.set(true);
                    fail(failure);
                });
            } catch (Throwable failure) {
                fail(failure);
                return;
            }
            synchronized (this) {
                if (!callbackCompleted.get() && inFlight) {
                    active = started;
                }
            }
        }

        private void startSessionLifecycle() {
            synchronized (this) {
                if (cancelled || publisherTerminated) {
                    termination.complete(null);
                    return;
                }
                if (lifecycleStarted) {
                    return;
                }
                lifecycleStarted = true;
            }
            try {
                Uni<Void> lifecycle = Objects.requireNonNull(sessionFactory.withSession(opened -> {
                    sessionOpened(opened);
                    return Uni.createFrom().completionStage(providerDone);
                }), "Hibernate Reactive session lifecycle Uni must not be null");
                lifecycle.subscribe().with(
                    ignored -> termination.complete(null),
                    this::lifecycleFailed);
            } catch (Throwable failure) {
                lifecycleFailed(failure);
            }
        }

        private void sessionOpened(Mutiny.Session opened) {
            synchronized (this) {
                inFlight = false;
                if (!cancelled && !publisherTerminated) {
                    session = opened;
                }
            }
            if (cancelled || publisherTerminated) {
                finishResources();
                return;
            }
            drain();
        }

        private void lifecycleFailed(Throwable failure) {
            boolean signal;
            synchronized (this) {
                signal = !cancelled && !publisherTerminated;
                publisherTerminated = true;
                inFlight = false;
            }
            if (signal) {
                signalError(failure);
            }
            termination.completeExceptionally(failure);
        }

        private void pageReceived(List<T> rows, int requestedWindow) {
            Objects.requireNonNull(rows, "Hibernate Reactive Query page must not be null");
            if (rows.size() > requestedWindow) {
                fail(new IllegalStateException("Hibernate Reactive Query page exceeded requested window"));
                return;
            }
            synchronized (this) {
                active = null;
                if (cancelled || publisherTerminated) {
                    inFlight = false;
                    return;
                }
            }
            try {
                for (T row : rows) {
                    if (!emitNext(row)) {
                        return;
                    }
                }
            } catch (Throwable failure) {
                fail(failure);
                return;
            }
            synchronized (this) {
                inFlight = false;
                if (cancelled || publisherTerminated) {
                    return;
                }
            }
            if (rows.size() < requestedWindow || semanticLimit.filter(value -> offset >= value).isPresent()) {
                complete();
            } else {
                drain();
            }
        }

        private int nextWindow() {
            long available = Math.min(requested.get(), maximumWindow);
            if (semanticLimit.isPresent()) {
                available = Math.min(available, semanticLimit.orElseThrow() - (long) offset);
            }
            return Math.toIntExact(Math.max(available, 0));
        }

        private void complete() {
            synchronized (this) {
                if (cancelled || publisherTerminated) {
                    return;
                }
                publisherTerminated = true;
                inFlight = false;
                active = null;
            }
            signalComplete();
            finishResources();
        }

        private void fail(Throwable failure) {
            synchronized (this) {
                if (cancelled || publisherTerminated) {
                    return;
                }
                publisherTerminated = true;
                inFlight = false;
                active = null;
            }
            signalError(Objects.requireNonNull(failure, "stream failure must not be null"));
            finishResources();
        }

        private boolean emitNext(T row) {
            synchronized (signalLock) {
                if (cancelled || publisherTerminated) {
                    return false;
                }
                if (row == null) {
                    throw new NullPointerException("Hibernate Reactive Query emitted a null row");
                }
                requested.decrementAndGet();
                offset = Math.addExact(offset, 1);
                emitting = true;
                try {
                    downstream.onNext(row);
                } finally {
                    emitting = false;
                    emitPendingTerminal();
                }
                return true;
            }
        }

        private void signalError(Throwable failure) {
            synchronized (signalLock) {
                if (emitting) {
                    pendingFailure = failure;
                    return;
                }
                downstream.onError(failure);
            }
        }

        private void signalComplete() {
            synchronized (signalLock) {
                if (emitting) {
                    pendingCompletion = true;
                    return;
                }
                downstream.onComplete();
            }
        }

        private void emitPendingTerminal() {
            if (pendingFailure != null) {
                Throwable failure = pendingFailure;
                pendingFailure = null;
                downstream.onError(failure);
            } else if (pendingCompletion) {
                pendingCompletion = false;
                downstream.onComplete();
            }
        }

        private void finishResources() {
            providerDone.complete(null);
            synchronized (this) {
                if (!lifecycleStarted) {
                    termination.complete(null);
                }
            }
        }

        private void addRequested(long count) {
            requested.updateAndGet(current -> {
                long updated = current + count;
                return updated < 0 ? Long.MAX_VALUE : updated;
            });
        }
    }
}
