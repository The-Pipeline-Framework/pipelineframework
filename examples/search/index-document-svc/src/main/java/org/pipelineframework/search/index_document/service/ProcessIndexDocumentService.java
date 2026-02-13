package org.pipelineframework.search.index_document.service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.service.ReactiveStreamingClientService;
import org.pipelineframework.step.NonRetryableException;

@PipelineStep(
    inputType = org.pipelineframework.search.common.domain.TokenBatch.class,
    outputType = org.pipelineframework.search.common.domain.IndexAck.class,
    stepType = org.pipelineframework.step.StepManyToOne.class,
    backendType = org.pipelineframework.grpc.GrpcServiceClientStreamingAdapter.class,
    inboundMapper = org.pipelineframework.search.common.mapper.TokenBatchMapper.class,
    outboundMapper = org.pipelineframework.search.common.mapper.IndexAckMapper.class,
    cacheKeyGenerator = org.pipelineframework.search.index_document.cache.IndexDocumentCacheKeyGenerator.class
)
/**
 * MANY_TO_ONE indexing reducer with explicit reliability semantics.
 *
 * <p>It classifies failures into transient vs non-retryable and parks exhausted failures for
 * operator visibility. The {@code __FAIL_TRANSIENT_N__} and {@code __FAIL_PERMANENT__} token markers are
 * intentional test/chaos-injection hooks used by reliability tests and should not be accepted from
 * untrusted external inputs.
 */
@ApplicationScoped
public class ProcessIndexDocumentService
    implements ReactiveStreamingClientService<TokenBatch, IndexAck> {
  private static final int MAX_BATCHES = 10_000;
  // This value represents retries after the initial attempt.
  private static final int DEFAULT_MAX_TRANSIENT_RETRIES = 3;
  private static final Duration DEFAULT_RETRY_WAIT = Duration.ofMillis(50);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.ofMillis(250);
  private static final String PERMANENT_FAILURE_MARKER = "__FAIL_PERMANENT__";
  private static final Pattern TRANSIENT_FAILURE_PATTERN = Pattern.compile("__FAIL_TRANSIENT_(\\d+)__");
  private static final Logger LOGGER = Logger.getLogger(ProcessIndexDocumentService.class);

  private final IndexFailureParkingLot parkingLot;
  private final int maxTransientRetries;
  private final Duration retryWait;
  private final Duration maxBackoff;
  private final boolean chaosEnabled;
  // Tracks transient marker attempts by docId and marker value; cleaned on success or terminal failure.
  private final ConcurrentMap<String, AtomicInteger> transientAttemptsByDoc = new ConcurrentHashMap<>();

  @Inject
  public ProcessIndexDocumentService(
      IndexFailureParkingLot parkingLot,
      @ConfigProperty(name = "search.index.chaos.enabled", defaultValue = "false") boolean chaosEnabled) {
    this(parkingLot, DEFAULT_MAX_TRANSIENT_RETRIES, DEFAULT_RETRY_WAIT, DEFAULT_MAX_BACKOFF, chaosEnabled);
  }

  ProcessIndexDocumentService(
      IndexFailureParkingLot parkingLot,
      int maxTransientRetries,
      Duration retryWait,
      Duration maxBackoff,
      boolean chaosEnabled) {
    IndexFailureParkingLot validatedParkingLot = Objects.requireNonNull(parkingLot, "parkingLot must not be null");
    if (maxTransientRetries < 0) {
      throw new IllegalArgumentException("maxTransientRetries must be >= 0, but was " + maxTransientRetries);
    }
    Duration validatedRetryWait = Objects.requireNonNull(retryWait, "retryWait must not be null");
    Duration validatedMaxBackoff = Objects.requireNonNull(maxBackoff, "maxBackoff must not be null");
    if (validatedRetryWait.compareTo(Duration.ZERO) <= 0) {
      throw new IllegalArgumentException("retryWait must be > 0, but was " + validatedRetryWait);
    }
    if (validatedMaxBackoff.compareTo(Duration.ZERO) <= 0) {
      throw new IllegalArgumentException("maxBackoff must be > 0, but was " + validatedMaxBackoff);
    }

    this.parkingLot = validatedParkingLot;
    this.maxTransientRetries = maxTransientRetries;
    this.retryWait = validatedRetryWait;
    this.maxBackoff = validatedMaxBackoff;
    this.chaosEnabled = chaosEnabled;
  }

  @Override
  public Uni<IndexAck> process(Multi<TokenBatch> input) {
    return input
        .collect()
        .asList()
        .onItem().transformToUni(this::processBatchesWithReliability);
  }

  private Uni<IndexAck> processBatchesWithReliability(List<TokenBatch> batches) {
    UUID docId = extractDocId(batches);
    String docIdLabel = docId == null ? "unknown-doc" : docId.toString();
    return Uni.createFrom().deferred(() -> processBatches(batches))
        .onFailure(TransientIndexingException.class)
        .retry()
        .withBackOff(retryWait, maxBackoff)
        .atMost(maxTransientRetries)
        .onFailure(this::isReliabilityFailure)
        .invoke(error -> {
          cleanupTransientAttempts(docIdLabel);
          parkingLot.park(docIdLabel, error.getClass().getSimpleName(), error.getMessage());
          LOGGER.errorf(error, "Indexing failed for docId %s and was parked", docIdLabel);
        })
        .onItem().invoke(ack -> {
          if (ack != null && ack.docId != null) {
            cleanupTransientAttempts(ack.docId.toString());
          }
        });
  }

  private boolean isReliabilityFailure(Throwable error) {
    return error instanceof TransientIndexingException || error instanceof NonRetryableException;
  }

  private Uni<IndexAck> processBatches(List<TokenBatch> batches) {
    if (batches == null || batches.isEmpty()) {
      return Uni.createFrom().failure(new IllegalArgumentException("token batches are required"));
    }
    if (batches.size() > MAX_BATCHES) {
      return Uni.createFrom()
          .failure(new IllegalArgumentException("token batch count exceeds limit: " + MAX_BATCHES));
    }
    if (batches.stream().anyMatch(batch ->
        batch == null
            || batch.tokens == null
            || batch.tokens.isBlank()
            || batch.tokensHash == null
            || batch.tokensHash.isBlank())) {
      return Uni.createFrom().failure(new IllegalArgumentException(
          "all token batches must contain tokens and tokensHash"));
    }
    UUID docId = batches.get(0).docId;
    if (docId == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("docId is required"));
    }
    if (batches.stream().anyMatch(batch -> batch.docId == null || !docId.equals(batch.docId))) {
      return Uni.createFrom().failure(new IllegalArgumentException("all token batches must share the same docId"));
    }
    FailureDirective directive = evaluateFailureDirective(batches, docId);
    if (directive != null) {
      return Uni.createFrom().failure(directive.toException());
    }

    String indexVersion = resolveIndexVersion();
    String joinedTokenHashes = batches.stream()
        .map(batch -> batch.tokensHash)
        .collect(Collectors.joining("|"));
    String combinedTokensHash = HashingUtils.sha256Base64Url(joinedTokenHashes);

    IndexAck output = new IndexAck();
    output.docId = docId;
    output.indexVersion = indexVersion;
    output.tokensHash = combinedTokensHash;
    output.indexedAt = Instant.now();
    output.success = true;

    LOGGER.debugf("Indexed doc %s from %s token batches (version=%s)", docId, batches.size(), indexVersion);
    return Uni.createFrom().item(output);
  }

  private UUID extractDocId(List<TokenBatch> batches) {
    if (batches == null || batches.isEmpty()) {
      return null;
    }
    return batches.get(0).docId;
  }

  /**
   * Evaluates test/chaos markers in token content and returns a directive when one is matched.
   *
   * <p>Only the first transient marker match found in order is applied. Permanent markers always
   * produce non-retryable failures. Marker evaluation is intentionally limited to reliability test
   * scenarios and should not be enabled for unsanitized external input.
   */
  private FailureDirective evaluateFailureDirective(List<TokenBatch> batches, UUID docId) {
    if (!chaosEnabled || docId == null || batches == null) {
      return null;
    }
    for (TokenBatch batch : batches) {
      if (batch == null || batch.tokens == null) {
        continue;
      }
      String tokens = batch.tokens;
      // Marker hooks are for controlled tests only; do not allow untrusted payloads to set these.
      if (tokens.contains(PERMANENT_FAILURE_MARKER)) {
        return FailureDirective.permanent("permanent indexing failure marker received");
      }
      Matcher matcher = TRANSIENT_FAILURE_PATTERN.matcher(tokens);
      if (matcher.find()) {
        int failAttempts = Integer.parseInt(matcher.group(1));
        String key = docId + ":" + failAttempts;
        AtomicInteger counter = transientAttemptsByDoc.computeIfAbsent(key, ignored -> new AtomicInteger(0));
        int currentAttempt = counter.incrementAndGet();
        if (currentAttempt <= failAttempts) {
          return FailureDirective.transientFailure(
              "transient indexing failure %d/%d".formatted(currentAttempt, failAttempts));
        }
        transientAttemptsByDoc.remove(key);
      }
    }
    return null;
  }

  private String resolveIndexVersion() {
    String configured = System.getenv("SEARCH_INDEX_VERSION");
    if (configured == null || configured.isBlank()) {
      return "v1";
    }
    return configured.trim();
  }

  private void cleanupTransientAttempts(String docIdLabel) {
    String prefix = docIdLabel + ":";
    transientAttemptsByDoc.keySet().removeIf(key -> key.startsWith(prefix));
  }

  private record FailureDirective(boolean transientFailure, String message) {
    static FailureDirective transientFailure(String message) {
      return new FailureDirective(true, message);
    }

    static FailureDirective permanent(String message) {
      return new FailureDirective(false, message);
    }

    RuntimeException toException() {
      if (transientFailure) {
        return new TransientIndexingException(message);
      }
      return new NonRetryableException(message);
    }
  }
}
