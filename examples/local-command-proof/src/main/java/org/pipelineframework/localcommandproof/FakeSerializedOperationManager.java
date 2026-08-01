package org.pipelineframework.localcommandproof;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.context.ThreadContext;
import org.pipelineframework.step.NonRetryableException;

/**
 * Application-owned blocking manager for proving explicit offload and serialization.
 */
@ApplicationScoped
public class FakeSerializedOperationManager {
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "local-command-proof-worker");
        thread.setDaemon(true);
        return thread;
    });
    private final Semaphore activePermit = new Semaphore(1);
    private final AtomicInteger activeOperations = new AtomicInteger();
    private final AtomicInteger maxActiveOperations = new AtomicInteger();
    private final AtomicInteger invocationCount = new AtomicInteger();
    private final AtomicReference<ObservedOperationCommand> lastInput = new AtomicReference<>();
    private final AtomicReference<Map<String, Object>> lastConfig = new AtomicReference<>(Map.of());
    private volatile CountDownLatch blockingOperationEntered = new CountDownLatch(0);
    private volatile CountDownLatch releaseBlockingOperation = new CountDownLatch(0);

    @Inject
    ThreadContext threadContext;

    public ExecutorService executor() {
        return executor;
    }

    public Executor workerExecutor() {
        return command -> executor.execute(threadContext.contextualRunnable(command));
    }

    public ObservedOperationResult executeBlocking(ObservedOperationCommand input, Map<String, Object> config) {
        boolean acquired = false;
        try {
            activePermit.acquire();
            acquired = true;
            int active = activeOperations.incrementAndGet();
            maxActiveOperations.accumulateAndGet(active, Math::max);
            invocationCount.incrementAndGet();
            lastInput.set(input);
            lastConfig.set(config == null ? Map.of() : Map.copyOf(config));

            if (input.behavior() == ObservedOperationCommand.Behavior.BLOCKING_SUCCESS) {
                blockingOperationEntered.countDown();
                if (!releaseBlockingOperation.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out waiting to release blocking operation");
                }
            }
            if (input.behavior() == ObservedOperationCommand.Behavior.RETRYABLE_FAILURE) {
                throw new IllegalStateException("retryable observed operation failure");
            }
            if (input.behavior() == ObservedOperationCommand.Behavior.NON_RETRYABLE_FAILURE) {
                throw new NonRetryableException("non-retryable observed operation failure");
            }
            return new ObservedOperationResult(
                input.operationId(),
                "OBSERVED:" + input.operationId(),
                Thread.currentThread().getName());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("observed operation interrupted", exception);
        } finally {
            if (acquired) {
                activeOperations.decrementAndGet();
                activePermit.release();
            }
        }
    }

    void prepareBlockingOperation() {
        blockingOperationEntered = new CountDownLatch(1);
        releaseBlockingOperation = new CountDownLatch(1);
    }

    boolean awaitBlockingOperation(long timeout, TimeUnit unit) throws InterruptedException {
        return blockingOperationEntered.await(timeout, unit);
    }

    void releaseBlockingOperation() {
        releaseBlockingOperation.countDown();
    }

    boolean awaitBlockingOperationEntered(long timeout, TimeUnit unit) throws InterruptedException {
        return blockingOperationEntered.await(timeout, unit);
    }

    void resetForTest() {
        releaseBlockingOperation();
        activeOperations.set(0);
        maxActiveOperations.set(0);
        invocationCount.set(0);
        lastInput.set(null);
        lastConfig.set(Map.of());
        blockingOperationEntered = new CountDownLatch(0);
        releaseBlockingOperation = new CountDownLatch(0);
    }

    int invocationCount() {
        return invocationCount.get();
    }

    int maxActiveOperations() {
        return maxActiveOperations.get();
    }

    ObservedOperationCommand lastInput() {
        return lastInput.get();
    }

    Map<String, Object> lastConfig() {
        return lastConfig.get();
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
