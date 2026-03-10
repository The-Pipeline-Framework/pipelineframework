package org.pipelineframework;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.grpc.Status;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.AbstractMultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.apache.commons.lang3.time.StopWatch;
import org.jboss.logging.Logger;
import org.pipelineframework.telemetry.ApmCompatibilityMetrics;
import org.pipelineframework.telemetry.PipelineTelemetry;
import org.pipelineframework.telemetry.RetryAmplificationGuard;
import org.pipelineframework.telemetry.RetryAmplificationGuardMode;
import org.pipelineframework.telemetry.RpcMetrics;

/**
 * Lifecycle hooks and retry-amplification guards for sync pipeline executions.
 */
@ApplicationScoped
class ExecutionHooks {

  private static final Logger LOG = Logger.getLogger(ExecutionHooks.class);
  private static final String ORCHESTRATOR_SERVICE = "OrchestratorService";
  private static final String ORCHESTRATOR_METHOD = "Run";

  @Inject
  PipelineTelemetry telemetry;

  private final ScheduledExecutorService killSwitchExecutor = Executors.newSingleThreadScheduledExecutor(
      runnable -> {
        Thread thread = new Thread(runnable, "tpf-kill-switch");
        thread.setDaemon(true);
        return thread;
      });

  @PreDestroy
  void shutdownKillSwitchExecutor() {
    killSwitchExecutor.shutdownNow();
  }

  <T> Multi<T> attachMultiHooks(Multi<T> multi, StopWatch watch) {
    Multi<T> guarded = attachRetryAmplificationGuard(multi);
    long[] startTime = new long[1];
    return guarded
        .onSubscription().invoke(ignored -> {
          LOG.info("PIPELINE BEGINS processing");
          startTime[0] = System.nanoTime();
          watch.start();
        })
        .onCompletion().invoke(() -> {
          watch.stop();
          long durationNanos = System.nanoTime() - startTime[0];
          RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.OK, durationNanos);
          ApmCompatibilityMetrics.recordOrchestratorSuccess(durationNanos / 1_000_000d);
          LOG.infof("✅ PIPELINE FINISHED processing in %s seconds", watch.getTime(TimeUnit.SECONDS));
        })
        .onFailure().invoke(failure -> {
          watch.stop();
          long durationNanos = System.nanoTime() - startTime[0];
          RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.fromThrowable(failure),
              durationNanos);
          ApmCompatibilityMetrics.recordOrchestratorFailure(durationNanos / 1_000_000d);
          LOG.errorf(failure, "❌ PIPELINE FAILED after %s seconds", watch.getTime(TimeUnit.SECONDS));
        });
  }

  <T> Uni<T> attachUniHooks(Uni<T> uni, StopWatch watch) {
    Uni<T> guarded = attachRetryAmplificationGuard(uni);
    long[] startTime = new long[1];
    return guarded
        .onSubscription().invoke(ignored -> {
          LOG.info("PIPELINE BEGINS processing");
          startTime[0] = System.nanoTime();
          watch.start();
        })
        .onItemOrFailure().invoke((item, failure) -> {
          watch.stop();
          long durationNanos = System.nanoTime() - startTime[0];
          if (failure == null) {
            RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.OK, durationNanos);
            ApmCompatibilityMetrics.recordOrchestratorSuccess(durationNanos / 1_000_000d);
            LOG.infof("✅ PIPELINE FINISHED processing in %s seconds", watch.getTime(TimeUnit.SECONDS));
          } else {
            RpcMetrics.recordGrpcServer(ORCHESTRATOR_SERVICE, ORCHESTRATOR_METHOD, Status.fromThrowable(failure),
                durationNanos);
            ApmCompatibilityMetrics.recordOrchestratorFailure(durationNanos / 1_000_000d);
            LOG.errorf(failure, "❌ PIPELINE FAILED after %s seconds", watch.getTime(TimeUnit.SECONDS));
          }
        });
  }

  private <T> Multi<T> attachRetryAmplificationGuard(Multi<T> multi) {
    if (telemetry == null || !telemetry.retryAmplificationGuardEnabled()) {
      return multi;
    }
    Duration interval = telemetry.retryAmplificationCheckInterval();
    RetryAmplificationGuardMode mode = telemetry.retryAmplificationMode();
    return multi.plug(upstream -> new RetryAmplificationGuardMulti<>(upstream, interval, mode));
  }

  private <T> Uni<T> attachRetryAmplificationGuard(Uni<T> uni) {
    if (telemetry == null || !telemetry.retryAmplificationGuardEnabled()) {
      return uni;
    }
    Duration interval = telemetry.retryAmplificationCheckInterval();
    RetryAmplificationGuardMode mode = telemetry.retryAmplificationMode();
    AtomicBoolean logged = new AtomicBoolean(false);
    return Uni.createFrom().emitter(emitter -> {
      AtomicReference<Cancellable> cancellableRef = new AtomicReference<>();
      AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();
      Cancellable cancellable = uni.subscribe().with(
          item -> {
            ScheduledFuture<?> scheduled = futureRef.get();
            if (scheduled != null) {
              scheduled.cancel(false);
            }
            emitter.complete(item);
          },
          failure -> {
            ScheduledFuture<?> scheduled = futureRef.get();
            if (scheduled != null) {
              scheduled.cancel(false);
            }
            emitter.fail(failure);
          });
      cancellableRef.set(cancellable);
      ScheduledFuture<?> future = killSwitchExecutor.scheduleAtFixedRate(() -> {
        telemetry.retryAmplificationTrigger().ifPresent(trigger -> {
          if (!logged.compareAndSet(false, true)) {
            return;
          }
          logRetryAmplificationTrigger(trigger, mode);
          if (mode == RetryAmplificationGuardMode.FAIL_FAST) {
            emitter.fail(PipelineKillSwitchException.retryAmplification(trigger));
            Cancellable active = cancellableRef.get();
            if (active != null) {
              active.cancel();
            }
          } else {
            ScheduledFuture<?> scheduled = futureRef.get();
            if (scheduled != null) {
              scheduled.cancel(false);
            }
          }
        });
      }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
      futureRef.set(future);
      emitter.onTermination(() -> {
        ScheduledFuture<?> scheduled = futureRef.get();
        if (scheduled != null) {
          scheduled.cancel(false);
        }
      });
    });
  }

  private void logRetryAmplificationTrigger(
      RetryAmplificationGuard.Trigger trigger,
      RetryAmplificationGuardMode mode) {
    String action = mode == RetryAmplificationGuardMode.FAIL_FAST ? "aborting" : "logging";
    LOG.errorf(
        "Retry amplification guard triggered for step %s (inflight slope %.2f > %.2f, retry rate %.2f) over %s (sustain samples %d); %s pipeline run.",
        trigger.step(),
        trigger.inflightSlope(),
        trigger.inflightSlopeThreshold(),
        trigger.retryRate(),
        trigger.window(),
        trigger.sustainSamples(),
        action);
  }

  private final class RetryAmplificationGuardMulti<T> extends AbstractMultiOperator<T, T> {
    private final Duration interval;
    private final RetryAmplificationGuardMode mode;

    private RetryAmplificationGuardMulti(
        Multi<? extends T> upstream,
        Duration interval,
        RetryAmplificationGuardMode mode) {
      super(upstream);
      this.interval = interval;
      this.mode = mode;
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> downstream) {
      RetryAmplificationProcessor<T> processor =
          new RetryAmplificationProcessor<>(downstream, interval, mode);
      upstream().subscribe(processor);
    }
  }

  private final class RetryAmplificationProcessor<T> extends MultiOperatorProcessor<T, T> {
    private final RetryAmplificationGuardMode mode;
    private final AtomicBoolean logged;
    private final ScheduledFuture<?> future;

    private RetryAmplificationProcessor(
        MultiSubscriber<? super T> downstream,
        Duration interval,
        RetryAmplificationGuardMode mode) {
      super(downstream);
      this.mode = mode;
      this.logged = new AtomicBoolean(false);
      this.future = killSwitchExecutor.scheduleAtFixedRate(
          this::checkGuard,
          interval.toMillis(),
          interval.toMillis(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    public void onItem(T item) {
      if (!isDone()) {
        downstream.onItem(item);
      }
    }

    @Override
    public void onFailure(Throwable failure) {
      cancelFuture();
      super.onFailure(failure);
    }

    @Override
    public void onCompletion() {
      cancelFuture();
      super.onCompletion();
    }

    @Override
    public void cancel() {
      cancelFuture();
      super.cancel();
    }

    private void checkGuard() {
      telemetry.retryAmplificationTrigger().ifPresent(trigger -> {
        if (!logged.compareAndSet(false, true)) {
          return;
        }
        logRetryAmplificationTrigger(trigger, mode);
        cancelFuture();
        if (mode == RetryAmplificationGuardMode.FAIL_FAST) {
          if (isDone()) {
            return;
          }
          super.onFailure(PipelineKillSwitchException.retryAmplification(trigger));
        }
      });
    }

    private void cancelFuture() {
      if (future != null) {
        future.cancel(false);
      }
    }
  }
}
