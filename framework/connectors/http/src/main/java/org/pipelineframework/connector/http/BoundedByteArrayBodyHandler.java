package org.pipelineframework.connector.http;

import java.io.ByteArrayOutputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cancels an HTTP response body as soon as its configured byte bound is exceeded. */
final class BoundedByteArrayBodyHandler implements HttpResponse.BodyHandler<byte[]> {
    private final int maximumBytes;

    BoundedByteArrayBodyHandler(int maximumBytes) {
        if (maximumBytes < 1) throw new IllegalArgumentException("maximum HTTP response bytes must be positive");
        this.maximumBytes = maximumBytes;
    }

    @Override
    public HttpResponse.BodySubscriber<byte[]> apply(HttpResponse.ResponseInfo responseInfo) {
        Objects.requireNonNull(responseInfo, "HTTP response info must not be null");
        return new Subscriber(maximumBytes);
    }

    private static final class Subscriber implements HttpResponse.BodySubscriber<byte[]> {
        private final int maximumBytes;
        private final CompletableFuture<byte[]> body = new CompletableFuture<>();
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final AtomicBoolean complete = new AtomicBoolean();
        private Flow.Subscription subscription;

        private Subscriber(int maximumBytes) {
            this.maximumBytes = maximumBytes;
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = Objects.requireNonNull(value, "HTTP body subscription must not be null");
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            if (complete.get()) return;
            try {
                for (ByteBuffer item : items) {
                    int size = item.remaining();
                    if (size > maximumBytes - bytes.size()) {
                        fail(new IllegalStateException("HTTP response exceeds configured size limit"));
                        return;
                    }
                    byte[] chunk = new byte[size];
                    item.get(chunk);
                    bytes.writeBytes(chunk);
                }
                subscription.request(1);
            } catch (RuntimeException failure) {
                fail(failure);
            }
        }

        @Override
        public void onError(Throwable failure) {
            if (complete.compareAndSet(false, true)) body.completeExceptionally(failure);
        }

        @Override
        public void onComplete() {
            if (complete.compareAndSet(false, true)) body.complete(bytes.toByteArray());
        }

        private void fail(RuntimeException failure) {
            if (complete.compareAndSet(false, true)) {
                subscription.cancel();
                body.completeExceptionally(failure);
            }
        }
    }
}
