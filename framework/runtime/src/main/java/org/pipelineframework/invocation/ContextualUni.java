package org.pipelineframework.invocation;

import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.AbstractUni;
import io.smallrye.mutiny.operators.UniOperator;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;

final class ContextualUni<T> extends UniOperator<T, T> {
    private final InvocationContextSnapshot context;
    private final PipelineInvocationStrategy strategy;
    private final long startNanos;

    ContextualUni(
        Uni<? extends T> upstream,
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
    public void subscribe(UniSubscriber<? super T> downstream) {
        ContextualUniSubscriber subscriber = new ContextualUniSubscriber(downstream);
        context.run(() -> AbstractUni.subscribe(upstream(), subscriber));
    }

    private final class ContextualUniSubscriber implements UniSubscriber<T>, UniSubscription {
        private final UniSubscriber<? super T> downstream;
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private volatile UniSubscription upstream;

        private ContextualUniSubscriber(UniSubscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(UniSubscription subscription) {
            this.upstream = subscription;
            context.run(() -> downstream.onSubscribe(this));
        }

        @Override
        public void onItem(T item) {
            if (terminated.compareAndSet(false, true)) {
                context.run(() -> {
                    try {
                        downstream.onItem(item);
                    } finally {
                        strategy.recordTermination(startNanos, null, false);
                    }
                });
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
        public void cancel() {
            if (terminated.compareAndSet(false, true)) {
                context.run(() -> {
                    UniSubscription current = upstream;
                    if (current != null) {
                        current.cancel();
                    }
                    strategy.recordTermination(startNanos, null, true);
                });
            }
        }
    }
}
