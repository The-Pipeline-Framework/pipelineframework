package org.pipelineframework.search.index_document.service;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.service.ReactiveStreamingClientService;
import org.pipelineframework.step.NonRetryableException;

@PipelineStep(cacheKeyGenerator = org.pipelineframework.search.index_document.cache.IndexDocumentCacheKeyGenerator.class)
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
    implements ReactiveStreamingClientService<EmbeddedChunk, IndexAck> {
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
  public Uni<IndexAck> process(Multi<EmbeddedChunk> input) {
    return input
        .collect()
        .asList()
        .onItem().transformToUni(this::processChunksWithReliability);
  }

  private Uni<IndexAck> processChunksWithReliability(List<EmbeddedChunk> chunks) {
    UUID docId = extractDocId(chunks);
    String docIdLabel = docId == null ? "unknown-doc" : docId.toString();
    return Uni.createFrom().deferred(() -> processChunks(chunks))
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

  private Uni<IndexAck> processChunks(List<EmbeddedChunk> chunks) {
    if (chunks == null || chunks.isEmpty()) {
      return Uni.createFrom().failure(new IllegalArgumentException("embedded chunks are required"));
    }
    if (chunks.size() > MAX_BATCHES) {
      return Uni.createFrom()
          .failure(new IllegalArgumentException("embedded chunk count exceeds limit: " + MAX_BATCHES));
    }
    if (chunks.stream().anyMatch(chunk ->
        chunk == null
            || chunk.tokens == null
            || chunk.tokens.isBlank()
            || chunk.tokensHash == null
            || chunk.tokensHash.isBlank()
            || chunk.vectorHash == null
            || chunk.vectorHash.isBlank()
            || chunk.vectorVersion == null
            || chunk.vectorVersion.isBlank())) {
      return Uni.createFrom().failure(new IllegalArgumentException(
          "all embedded chunks must contain tokens, tokensHash, vectorHash, and vectorVersion"));
    }
    UUID docId = chunks.get(0).docId;
    if (docId == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("docId is required"));
    }
    if (chunks.stream().anyMatch(chunk -> chunk.docId == null || !docId.equals(chunk.docId))) {
      return Uni.createFrom().failure(new IllegalArgumentException("all embedded chunks must share the same docId"));
    }
    if (chunks.stream().anyMatch(chunk ->
        chunk.batchIndex == null || chunk.batchIndex < 0 || chunk.tokenCount == null || chunk.tokenCount <= 0)) {
      return Uni.createFrom().failure(new IllegalArgumentException(
          "invalid embedded chunk metrics for docId " + docId + ": batchIndex must be >= 0 and tokenCount must be > 0"));
    }
    List<EmbeddedChunk> orderedChunks = orderChunksForAggregation(chunks);
    FailureDirective directive = evaluateFailureDirective(orderedChunks, docId);
    if (directive != null) {
      return Uni.createFrom().failure(directive.toException());
    }

    String indexVersion = resolveIndexVersion();
    String joinedVectorHashes = orderedChunks.stream()
        .map(chunk -> chunk.vectorHash)
        .collect(Collectors.joining("|"));
    String combinedTokensHash = HashingUtils.sha256Base64Url(joinedVectorHashes);
    AggregationSummary summary = summarizeTokens(orderedChunks);

    IndexAck output = new IndexAck();
    output.docId = docId;
    output.setIndexVersion(indexVersion);
    output.setTokensHash(combinedTokensHash);
    output.setTokenBatchCount(orderedChunks.size());
    output.setUniqueTokenCount(summary.uniqueTokenCount());
    output.setTopToken(summary.topToken());
    output.setIndexedAt(Instant.now());
    output.setSuccess(true);

    LOGGER.debugf(
        "Indexed doc %s from %s embedded chunks (version=%s, uniqueTokens=%s, topToken=%s)",
        docId,
        orderedChunks.size(),
        indexVersion,
        summary.uniqueTokenCount(),
        summary.topToken());
    return Uni.createFrom().item(output);
  }

  private UUID extractDocId(List<EmbeddedChunk> chunks) {
    if (chunks == null || chunks.isEmpty()) {
      return null;
    }
    return chunks.get(0).docId;
  }

  /**
   * Evaluates test/chaos markers in token content and returns a directive when one is matched.
   *
   * <p>Only the first transient marker match found in order is applied. Permanent markers always
   * produce non-retryable failures. Marker evaluation is intentionally limited to reliability test
   * scenarios and should not be enabled for unsanitized external input.
   */
  private FailureDirective evaluateFailureDirective(List<EmbeddedChunk> chunks, UUID docId) {
    if (!chaosEnabled || docId == null || chunks == null) {
      return null;
    }
    for (EmbeddedChunk chunk : chunks) {
      if (chunk == null || chunk.tokens == null) {
        continue;
      }
      String tokens = chunk.tokens;
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

  private List<EmbeddedChunk> orderChunksForAggregation(List<EmbeddedChunk> chunks) {
    return chunks.stream()
        .sorted(Comparator
            .comparing((EmbeddedChunk chunk) -> chunk.batchIndex, Comparator.nullsLast(Integer::compareTo))
            .thenComparing(chunk -> chunk.vectorHash, Comparator.nullsLast(String::compareTo))
            .thenComparing(chunk -> chunk.tokens, Comparator.nullsLast(String::compareTo)))
        .toList();
  }

  /**
   * Produces a summary of tokens found across the given embedded chunks.
   *
   * Tokens are extracted by splitting each batch's `tokens` text on whitespace; null or blank token strings are ignored.
   *
   * @param chunks the embedded chunks to aggregate
   * @return an AggregationSummary whose first element is the number of distinct tokens and whose second element is the most frequent token;
   *         if no tokens are present the unique token count is 0 and the top token is `null`. The top token is chosen by highest frequency,
   *         with ties broken by selecting the lexicographically smaller token (earlier in natural string order).
   */
  private AggregationSummary summarizeTokens(List<EmbeddedChunk> chunks) {
    Map<String, Integer> counts = new HashMap<>();
    for (EmbeddedChunk chunk : chunks) {
      if (chunk.tokens == null || chunk.tokens.isBlank()) {
        continue;
      }
      for (String token : chunk.tokens.trim().split("\\s+")) {
        if (token.isBlank()) {
          continue;
        }
        counts.merge(token, 1, Integer::sum);
      }
    }
    if (counts.isEmpty()) {
      return new AggregationSummary(0, null);
    }
    String topToken = counts.entrySet().stream()
        .max(Comparator.<Map.Entry<String, Integer>>comparingInt(Map.Entry::getValue)
            .thenComparing(Map.Entry::getKey, Comparator.reverseOrder()))
        .map(Map.Entry::getKey)
        .orElse(null);
    return new AggregationSummary(counts.size(), topToken);
  }

  private record AggregationSummary(int uniqueTokenCount, String topToken) {}

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
