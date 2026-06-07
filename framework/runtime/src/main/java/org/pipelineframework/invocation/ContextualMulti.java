package org.pipelineframework.invocation;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.MultiOperator;
import io.smallrye.mutiny.subscription.MultiSubscriber;

final class ContextualMulti<T> extends MultiOperator<T, T> {
    private final InvocationContextSnapshot context;
    private final PipelineInvocationStrategy strategy;
    private final long startNanos;

    ContextualMulti(
        Multi<T> upstream,
        InvocationContextSnapshot context,
        PipelineInvocationStrategy strategy,
        long startNanos
    ) {
        super(upstream);
        this.context = context;
        this.strategy = strategy;
        this.startNanos = startNanos;
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> downstream) {
        ContextualMultiSubscriber subscriber = new ContextualMultiSubscriber(downstream);
        context.run(() -> upstream().subscribe(subscriber));
    }

    private final class ContextualMultiSubscriber implements MultiSubscriber<T>, Flow.Subscription {
        private final MultiSubscriber<? super T> downstream;
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private volatile Flow.Subscription upstream;

        private ContextualMultiSubscriber(MultiSubscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.upstream = subscription;
            context.run(() -> downstream.onSubscribe(this));
        }

        @Override
        public void onItem(T item) {
            if (!terminated.get()) {
                context.run(() -> downstream.onItem(item));
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (terminated.compareAndSet(false, true)) {
                context.run(() -> {
                    try {
                        downstream.onFailure(failure);
                    } finally {
                        strategy.recordTermination(startNanos, failure, false);
                    }
                });
            }
        }

        @Override
        public void onCompletion() {
            if (terminated.compareAndSet(false, true)) {
                context.run(() -> {
                    try {
                        downstream.onCompletion();
                    } finally {
                        strategy.recordTermination(startNanos, null, false);
                    }
                });
            }
        }

        @Override
        public void request(long n) {
            context.run(() -> {
                Flow.Subscription current = upstream;
                if (current != null) {
                    current.request(n);
                }
            });
        }

        @Override
        public void cancel() {
            if (terminated.compareAndSet(false, true)) {
                context.run(() -> {
                    Flow.Subscription current = upstream;
                    if (current != null) {
                        current.cancel();
                    }
                    strategy.recordTermination(startNanos, null, true);
                });
            }
        }
    }
}
