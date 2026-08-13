package org.pipelineframework.orchestrator;

import java.util.Optional;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.telemetry.observation.TransitionObservation;
import org.pipelineframework.telemetry.derivation.TransitionTelemetryDerivation;

/**
 * Bounded executor for queue-async transition workers.
 */
@ApplicationScoped
public class TransitionWorkerExecutor {

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    @Inject
    TransitionWorkerMetrics telemetry = TransitionWorkerMetrics.disabled();

    @Inject
    TransitionWorkerTracing tracing = TransitionWorkerTracing.disabled();

    private final Object lifecycleLock = new Object();
    private volatile Semaphore permits;
    private volatile int maxInFlight;
    private volatile ExecutorService virtualThreadExecutor;

    /**
     * Default constructor for CDI.
     */
    public TransitionWorkerExecutor() {
    }

    /**
     * Test constructor.
     *
     * @param orchestratorConfig orchestrator config
     */
    public TransitionWorkerExecutor(PipelineOrchestratorConfig orchestratorConfig) {
        this(orchestratorConfig, null);
    }

    public TransitionWorkerExecutor(PipelineOrchestratorConfig orchestratorConfig, PipelineInvocationRuntime invocationRuntime) {
        this.orchestratorConfig = orchestratorConfig;
        this.invocationRuntime = invocationRuntime;
    }

    /**
     * Attempts to admit one transition without blocking.
     *
     * @return admission permit when capacity is available
     */
    public Optional<TransitionAdmission> tryAdmit() {
        Semaphore activePermits = permits();
        if (!activePermits.tryAcquire()) {
            record(new TransitionObservation.Saturated(Instant.now()));
            return Optional.empty();
        }
        record(new TransitionObservation.Admitted(Instant.now()));
        return Optional.of(new TransitionAdmission(this, activePermits));
    }

    /**
     * Executes a transition worker for an already-admitted command.
     *
     * @param worker transition worker
     * @param command transition command
     * @return worker result envelope
     */
    public Uni<TransitionResultEnvelope> execute(
        PipelineTransitionWorker worker,
        TransitionCommandEnvelope command) {
        Uni<TransitionResultEnvelope> execution = invocationRuntime().invokeTransitionWorker(
            duration -> record(new TransitionObservation.DurationRecorded(duration, Instant.now())),
            () -> {
                record(new TransitionObservation.Dispatched(Instant.now()));
                try {
                    Uni<TransitionResultEnvelope> result = worker.executeTransition(command);
                    if (result == null) {
                        record(new TransitionObservation.OutcomeRecorded(
                            TransitionWorkerOutcome.FAILED.name(), Instant.now()));
                        return Uni.createFrom().item(TransitionResultEnvelope.failed(
                            new IllegalStateException("PipelineTransitionWorker returned null")));
                    }
                    return result
                        .onItem().invoke(item -> record(new TransitionObservation.OutcomeRecorded(
                            item.outcome().name(), Instant.now())))
                        .onFailure().invoke(failure -> record(new TransitionObservation.OutcomeRecorded(
                            TransitionWorkerOutcome.FAILED.name(), Instant.now())));
                } catch (Exception failure) {
                    record(new TransitionObservation.OutcomeRecorded(
                        TransitionWorkerOutcome.FAILED.name(), Instant.now()));
                    return Uni.createFrom().item(TransitionResultEnvelope.failed(failure));
                }
            });
        if (executionMode() == TransitionWorkerExecutionMode.VIRTUAL_THREAD) {
            return execution.runSubscriptionOn(virtualThreadExecutor());
        }
        return execution;
    }

    public int activePermits() {
        Semaphore activePermits = permits;
        int configuredMax = maxInFlight;
        return activePermits == null ? 0 : configuredMax - activePermits.availablePermits();
    }

    private Semaphore permits() {
        Semaphore active = permits;
        if (active != null) {
            return active;
        }
        synchronized (lifecycleLock) {
            if (permits == null) {
                int configured = configuredMaxInFlight();
                maxInFlight = configured;
                permits = new Semaphore(configured);
            }
            return permits;
        }
    }

    private int configuredMaxInFlight() {
        PipelineOrchestratorConfig.WorkerConfig worker = workerConfig();
        if (worker == null) {
            return 64;
        }
        return Math.max(1, worker.maxInFlight());
    }

    private TransitionWorkerExecutionMode executionMode() {
        PipelineOrchestratorConfig.WorkerConfig worker = workerConfig();
        if (worker == null || worker.executionMode() == null) {
            return TransitionWorkerExecutionMode.SAME_THREAD;
        }
        return worker.executionMode();
    }

    private PipelineOrchestratorConfig.WorkerConfig workerConfig() {
        return orchestratorConfig == null ? null : orchestratorConfig.worker();
    }

    private PipelineInvocationRuntime invocationRuntime() {
        if (invocationRuntime == null) {
            throw new IllegalStateException("PipelineInvocationRuntime was not injected into "
                + "TransitionWorkerExecutor.invocationRuntime");
        }
        return invocationRuntime;
    }

    private TransitionWorkerMetrics telemetry() {
        return telemetry == null ? TransitionWorkerMetrics.disabled() : telemetry;
    }

    private void record(TransitionObservation observation) {
        TransitionTelemetryDerivation.metrics(observation).forEach(telemetry()::record);
        TransitionTelemetryDerivation.span(observation)
            .ifPresent((tracing == null ? TransitionWorkerTracing.disabled() : tracing)::record);
    }

    private Executor virtualThreadExecutor() {
        ExecutorService active = virtualThreadExecutor;
        if (active != null) {
            return active;
        }
        synchronized (lifecycleLock) {
            if (virtualThreadExecutor == null) {
                virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
            }
            return virtualThreadExecutor;
        }
    }

    private void release(Semaphore acquiredPermits) {
        acquiredPermits.release();
        record(new TransitionObservation.Released(Instant.now()));
    }

    @PreDestroy
    void shutdown() {
        ExecutorService active = virtualThreadExecutor;
        if (active != null) {
            active.shutdownNow();
        }
    }

    /**
     * Idempotent admission permit.
     */
    public static final class TransitionAdmission implements AutoCloseable {
        private final TransitionWorkerExecutor owner;
        private final Semaphore acquiredPermits;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private TransitionAdmission(TransitionWorkerExecutor owner, Semaphore acquiredPermits) {
            this.owner = owner;
            this.acquiredPermits = acquiredPermits;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.release(acquiredPermits);
            }
        }
    }
}
