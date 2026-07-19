package org.pipelineframework;

import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelinePlatformResourceLoader;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.runtime.core.resilience.CircuitOpenException;
import org.pipelineframework.runtime.core.resilience.CircuitProtectionUnavailableException;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.step.PipelineControlFlowException;

/**
 * Retry and terminal-failure policy for async execution transitions.
 */
@ApplicationScoped
class ExecutionFailureHandler {

  private static final Logger LOG = Logger.getLogger(ExecutionFailureHandler.class);
  private static final String ORCHESTRATOR_SERVICE = "OrchestratorService";
  private static final String ORCHESTRATOR_METHOD = "Run";

  @Inject
  PipelineOrchestratorConfig orchestratorConfig;

  @Inject
  SegmentBoundaryLedger segmentBoundaryLedger;

  Uni<Void> handleExecutionFailure(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      Throwable failure,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      DeadLetterPublisher deadLetterPublisher) {
    long now = System.currentTimeMillis();
    Optional<CircuitDeferral> circuitDeferral = circuitDeferral(failure);
    if (circuitDeferral.isPresent()) {
      return deferCircuit(record, transitionKey, circuitDeferral.orElseThrow(), executionStateStore, workDispatcher,
          deadLetterPublisher, now);
    }
    int nextAttempt = record.attempt() + 1;
    FailureClassification classification = classifyFailure(failure);
    Throwable classifiedFailure = classification.classifiedThrowable();
    boolean retryableFailure = classification.retryable();
    boolean retryAllowed = nextAttempt <= orchestratorConfig.maxRetries();

    if (retryAllowed && retryableFailure) {
      long nextDue = now + retryDelayMillis(nextAttempt);
      LOG.warnf(
          classifiedFailure,
          "Scheduling async execution retry execution=%s tenant=%s transition=%s nextAttempt=%d delayMs=%d error=%s",
          record.executionId(),
          record.tenantId(),
          transitionKey,
          nextAttempt,
          Math.max(0L, nextDue - now),
          classifiedFailure.getMessage());
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
          return segmentBoundaryLedger()
              .recordRunFailed(
                  updated.get(),
                  classifiedFailure.getClass().getSimpleName(),
                  classifiedFailure.getMessage(),
                  now)
              .chain(() -> deadLetterPublisher.publish(envelope));
        });
  }

  private Uni<Void> deferCircuit(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      CircuitDeferral circuitDeferral,
      ExecutionStateStore executionStateStore,
      WorkDispatcher workDispatcher,
      DeadLetterPublisher deadLetterPublisher,
      long now) {
    Duration maximum = orchestratorConfig.maxCircuitDeferral().orElse(Duration.ZERO);
    long firstDeferredAt = record.firstCircuitDeferredAtEpochMs() > 0
        ? record.firstCircuitDeferredAtEpochMs()
        : now;
    if (maximum.isZero() || maximum.isNegative() || now - firstDeferredAt >= maximum.toMillis()) {
      return terminalCircuitDeferral(record, transitionKey, circuitDeferral, executionStateStore, deadLetterPublisher, now);
    }
    long retryDecision = now + retryDelayMillis(Math.max(1, record.attempt() + 1));
    long nextDue = Math.max(retryDecision, circuitDeferral.notBeforeEpochMs());
    return executionStateStore.deferCircuit(
            record.tenantId(), record.executionId(), record.version(), nextDue, transitionKey,
            circuitDeferral.identity(), circuitDeferral.reason(), circuitDeferral.message(), firstDeferredAt,
            record.circuitDeferralCount() + 1, now)
        .onItem().transformToUni(updated -> {
          if (updated.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          return workDispatcher.enqueueDelayed(new ExecutionWorkItem(record.tenantId(), record.executionId()),
              Duration.ofMillis(Math.max(0L, nextDue - System.currentTimeMillis())));
        });
  }

  private Uni<Void> terminalCircuitDeferral(
      ExecutionRecord<Object, Object> record,
      String transitionKey,
      CircuitDeferral circuitDeferral,
      ExecutionStateStore executionStateStore,
      DeadLetterPublisher deadLetterPublisher,
      long now) {
    return executionStateStore.markTerminalFailure(
            record.tenantId(), record.executionId(), record.version(), ExecutionStatus.FAILED, transitionKey,
            "circuit_deferral_exhausted", circuitDeferral.message(), now)
        .onItem().transformToUni(updated -> {
          if (updated.isEmpty()) {
            return Uni.createFrom().voidItem();
          }
          PipelinePlatformResourceLoader.PlatformMetadata metadata = PipelinePlatformResourceLoader.loadPlatform().orElse(null);
          DeadLetterEnvelope envelope = DeadLetterEnvelope.builder()
              .tenantId(record.tenantId())
              .executionId(record.executionId())
              .executionKey(record.executionKey())
              .correlationId(record.executionKey())
              .transitionKey(transitionKey)
              .resourceType("tpf.orchestrator.execution")
              .resourceName(ORCHESTRATOR_SERVICE + "/" + ORCHESTRATOR_METHOD)
              .transport(resolveTransport(metadata))
              .platform(resolvePlatform(metadata))
              .terminalStatus(ExecutionStatus.FAILED.name())
              .terminalReason("circuit_deferral_exhausted")
              .errorCode("circuit_deferral_exhausted")
              .errorMessage(circuitDeferral.message())
              .retryable(false)
              .retriesObserved(record.attempt())
              .createdAtEpochMs(now)
              .build();
          return segmentBoundaryLedger()
              .recordRunFailed(updated.orElseThrow(), "circuit_deferral_exhausted", circuitDeferral.message(), now)
              .chain(() -> deadLetterPublisher.publish(envelope));
        });
  }

  private SegmentBoundaryLedger segmentBoundaryLedger() {
    return segmentBoundaryLedger == null ? new SegmentBoundaryLedger() : segmentBoundaryLedger;
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
    Throwable controlFlow = findThrowable(failure, PipelineControlFlowException.class);
    if (controlFlow != null) {
      return new FailureClassification(false, controlFlow);
    }
    return new FailureClassification(true, failure);
  }

  private static Optional<CircuitDeferral> circuitDeferral(Throwable failure) {
    if (failure == null) {
      return Optional.empty();
    }
    CircuitOpenException open = (CircuitOpenException) findThrowable(failure, CircuitOpenException.class);
    if (open != null) {
      return Optional.of(new CircuitDeferral(
          open.circuitOpen().identity().value(),
          "circuit_open",
          open.getMessage(),
          open.circuitOpen().notBefore().toEpochMilli()));
    }
    CircuitProtectionUnavailableException unavailable = (CircuitProtectionUnavailableException) findThrowable(
        failure, CircuitProtectionUnavailableException.class);
    if (unavailable != null) {
      return Optional.of(new CircuitDeferral(
          unavailable.protection().identity().value(),
          "circuit_protection_unavailable",
          unavailable.getMessage(),
          unavailable.protection().notBefore().toEpochMilli()));
    }
    return Optional.empty();
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
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed != null && suppressed != current) {
          queue.add(suppressed);
        }
      }
      if (current instanceof CompositeException composite) {
        for (Throwable causeItem : composite.getCauses()) {
          if (causeItem != null && causeItem != current) {
            queue.add(causeItem);
          }
        }
      }
    }
    return null;
  }

  record FailureClassification(boolean retryable, Throwable classifiedThrowable) {
  }

  private record CircuitDeferral(String identity, String reason, String message, long notBeforeEpochMs) {
  }
}
