package org.pipelineframework;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelinePlatformResourceLoader;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.step.NonRetryableException;

/**
 * Retry and terminal-failure policy for async execution transitions.
 */
@ApplicationScoped
class ExecutionFailureHandler {

  private static final String ORCHESTRATOR_SERVICE = "OrchestratorService";
  private static final String ORCHESTRATOR_METHOD = "Run";

  @Inject
  PipelineOrchestratorConfig orchestratorConfig;

  Uni<Void> handleExecutionFailure(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      Throwable failure,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      DeadLetterPublisher deadLetterPublisher) {
    long now = System.currentTimeMillis();
    int nextAttempt = record.attempt() + 1;
    FailureClassification classification = classifyFailure(failure);
    Throwable classifiedFailure = classification.classifiedThrowable();
    boolean retryableFailure = classification.retryable();
    boolean retryAllowed = nextAttempt <= orchestratorConfig.maxRetries();

    if (retryAllowed && retryableFailure) {
      long nextDue = now + retryDelayMillis(nextAttempt);
      return executionStateStore.scheduleRetry(
              record.tenantId(),
              record.executionId(),
              record.version(),
              nextAttempt,
              nextDue,
              transitionKey,
              classifiedFailure.getClass().getSimpleName(),
              classifiedFailure.getMessage(),
              now)
          .onItem().transformToUni(updated -> {
            if (updated.isEmpty()) {
              return Uni.createFrom().voidItem();
            }
            Duration delay = Duration.ofMillis(Math.max(0L, nextDue - System.currentTimeMillis()));
            return workDispatcher.enqueueDelayed(
                new ExecutionWorkItem(record.tenantId(), record.executionId()),
                delay);
          });
    }

    return executionStateStore.markTerminalFailure(
            record.tenantId(),
            record.executionId(),
            record.version(),
            ExecutionStatus.FAILED,
            transitionKey,
            classifiedFailure.getClass().getSimpleName(),
            classifiedFailure.getMessage(),
            now)
        .onItem().transformToUni(updated -> {
          if (updated.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          PipelinePlatformResourceLoader.PlatformMetadata platformMetadata = PipelinePlatformResourceLoader
              .loadPlatform()
              .orElse(null);
          DeadLetterEnvelope envelope = DeadLetterEnvelope.builder()
              .tenantId(record.tenantId())
              .executionId(record.executionId())
              .executionKey(record.executionKey())
              .correlationId(record.executionKey())
              .transitionKey(transitionKey)
              .resourceType("tpf.orchestrator.execution")
              .resourceName(ORCHESTRATOR_SERVICE + "/" + ORCHESTRATOR_METHOD)
              .transport(resolveTransport(platformMetadata))
              .platform(resolvePlatform(platformMetadata))
              .terminalStatus(ExecutionStatus.FAILED.name())
              .terminalReason(retryableFailure ? "retry_exhausted" : "non_retryable")
              .errorCode(classifiedFailure.getClass().getSimpleName())
              .errorMessage(classifiedFailure.getMessage())
              .retryable(retryableFailure)
              .retriesObserved(record.attempt())
              .createdAtEpochMs(now)
              .build();
          return deadLetterPublisher.publish(envelope);
        });
  }

  long retryDelayMillis(int nextAttempt) {
    long base = Math.max(0L, orchestratorConfig.retryDelay().toMillis());
    double multiplier = Math.max(1.0d, orchestratorConfig.retryMultiplier());
    double calculated = base * Math.pow(multiplier, Math.max(0, nextAttempt - 1));
    return Math.min((long) calculated, TimeUnit.MINUTES.toMillis(30));
  }

  private static String resolveTransport(PipelinePlatformResourceLoader.PlatformMetadata metadata) {
    String candidate = metadata == null ? System.getProperty("pipeline.transport") : metadata.transport();
    if (candidate == null || candidate.isBlank()) {
      return "UNKNOWN";
    }
    return candidate.trim().toUpperCase(Locale.ROOT);
  }

  private static String resolvePlatform(PipelinePlatformResourceLoader.PlatformMetadata metadata) {
    String candidate = metadata == null ? System.getProperty("pipeline.platform") : metadata.platform();
    if (candidate == null || candidate.isBlank()) {
      return "UNKNOWN";
    }
    return candidate.trim().toUpperCase(Locale.ROOT);
  }

  private static FailureClassification classifyFailure(Throwable failure) {
    if (failure == null) {
      Throwable classified = new IllegalStateException("Unknown failure");
      return new FailureClassification(false, classified);
    }
    Throwable nonRetryable = findThrowable(failure, NonRetryableException.class);
    if (nonRetryable != null) {
      return new FailureClassification(false, nonRetryable);
    }
    return new FailureClassification(true, failure);
  }

  private static Throwable findThrowable(Throwable failure, Class<? extends Throwable> targetType) {
    java.util.ArrayDeque<Throwable> queue = new java.util.ArrayDeque<>();
    java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    queue.add(failure);
    while (!queue.isEmpty()) {
      Throwable current = queue.removeFirst();
      if (!seen.add(current)) {
        continue;
      }
      if (targetType.isInstance(current)) {
        return current;
      }
      Throwable cause = current.getCause();
      if (cause != null && cause != current) {
        queue.add(cause);
      }
    }
    return null;
  }

  record FailureClassification(boolean retryable, Throwable classifiedThrowable) {
  }
}
