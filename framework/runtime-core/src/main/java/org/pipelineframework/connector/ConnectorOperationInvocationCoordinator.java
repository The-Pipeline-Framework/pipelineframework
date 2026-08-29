package org.pipelineframework.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Applies the execution and concurrency semantics declared by connector operation interfaces.
 *
 * <p>Serialization is keyed by binding name plus operation ID and major version. Separate bindings
 * remain independent without introducing provider-wide or connection-wide concurrency metadata.
 */
public final class ConnectorOperationInvocationCoordinator {
    private final Map<InvocationKey, CompletableFuture<Void>> serializedTails = new HashMap<>();

    public <T> CompletionStage<T> invoke(
        ConnectorBindingName binding,
        ConnectorOperation operation,
        Supplier<? extends CompletionStage<T>> invocation
    ) {
        Objects.requireNonNull(binding, "connector binding must not be null");
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(invocation, "invocation must not be null");
        if (!(operation instanceof SerializedOperation)) {
            return invokeOperation(operation, invocation);
        }
        return invokeSerialized(binding, operation, invocation);
    }

    private <T> CompletionStage<T> invokeSerialized(
        ConnectorBindingName binding,
        ConnectorOperation operation,
        Supplier<? extends CompletionStage<T>> invocation
    ) {
        InvocationKey key = new InvocationKey(binding, operation.id(), operation.majorVersion());
        CompletableFuture<Void> turn = new CompletableFuture<>();
        CompletableFuture<Void> predecessor;
        synchronized (serializedTails) {
            predecessor = serializedTails.put(key, turn);
        }
        SerializedInvocationFuture<T> result = new SerializedInvocationFuture<>();
        CompletionStage<Void> ready = predecessor == null
            ? CompletableFuture.completedFuture(null)
            : predecessor;
        ready.whenComplete((ignored, predecessorFailure) -> {
            if (!result.start()) {
                release(key, turn);
                return;
            }
            try {
                if (operation instanceof BlockingOperation) {
                    CompletionStage<CompletionStage<T>> admission = RuntimeAdapters.executeBlocking(
                        () -> requireStage(invocation.get()), false);
                    admission.whenComplete((providerStage, failure) -> {
                        if (failure == null) {
                            observeProviderStage(key, turn, result, providerStage);
                        } else {
                            failAndRelease(key, turn, result, failure);
                        }
                    });
                } else {
                    observeProviderStage(key, turn, result, invokeOperation(operation, invocation));
                }
            } catch (Throwable failure) {
                failAndRelease(key, turn, result, failure);
            }
        });
        return result;
    }

    private <T> void observeProviderStage(
        InvocationKey key,
        CompletableFuture<Void> turn,
        SerializedInvocationFuture<T> result,
        CompletionStage<T> providerStage
    ) {
        try {
            result.providerStage(providerStage);
            providerStage.whenComplete((value, failure) -> {
                if (!result.isCancelled()) {
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(failure);
                    }
                }
                release(key, turn);
            });
        } catch (Throwable failure) {
            failAndRelease(key, turn, result, failure);
        }
    }

    private <T> void failAndRelease(
        InvocationKey key,
        CompletableFuture<Void> turn,
        SerializedInvocationFuture<T> result,
        Throwable failure
    ) {
        if (!result.isCancelled()) {
            result.completeExceptionally(failure);
        }
        release(key, turn);
    }

    private void release(InvocationKey key, CompletableFuture<Void> turn) {
        synchronized (serializedTails) {
            serializedTails.remove(key, turn);
        }
        turn.complete(null);
    }

    private record InvocationKey(ConnectorBindingName binding, String operationId, int majorVersion) {
    }

    private static <T> CompletionStage<T> invokeOperation(
        ConnectorOperation operation,
        Supplier<? extends CompletionStage<T>> invocation
    ) {
        if (operation instanceof BlockingOperation) {
            return RuntimeAdapters.executeBlocking(invocation::get, false)
                .thenCompose(ConnectorOperationInvocationCoordinator::requireStage);
        }
        try {
            return requireStage(invocation.get());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static <T> CompletionStage<T> requireStage(CompletionStage<T> stage) {
        if (stage == null) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("connector operation returned a null CompletionStage"));
        }
        return stage;
    }

    private static final class SerializedInvocationFuture<T> extends CompletableFuture<T> {
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicReference<CompletionStage<T>> providerStage = new AtomicReference<>();

        private boolean start() {
            return !isCancelled() && started.compareAndSet(false, true);
        }

        private void providerStage(CompletionStage<T> stage) {
            providerStage.set(stage);
            if (isCancelled()) {
                cancelProvider(stage, true);
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            CompletionStage<T> stage = providerStage.get();
            if (cancelled && started.get() && stage != null) {
                cancelProvider(stage, mayInterruptIfRunning);
            }
            return cancelled;
        }

        private static void cancelProvider(CompletionStage<?> stage, boolean mayInterruptIfRunning) {
            try {
                stage.toCompletableFuture().cancel(mayInterruptIfRunning);
            } catch (RuntimeException ignored) {
                // CompletionStage cancellation is optional; forwarding is best-effort.
            }
        }
    }
}
