package org.pipelineframework.search.index_document.service;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
public class IndexFailureParkingLot {
  private static final int MAX_CAPACITY = 1_000;
  private static final Logger LOGGER = Logger.getLogger(IndexFailureParkingLot.class);

  private final Deque<ParkedFailure> parkedFailures = new ArrayDeque<>();

  public synchronized void park(String docId, String errorType, String reason) {
    if (parkedFailures.size() >= MAX_CAPACITY) {
      ParkedFailure evicted = parkedFailures.pollFirst();
      if (evicted != null) {
        LOGGER.warnf(
            "Index failure parking lot at capacity (%d): evicting oldest failure for docId=%s",
            MAX_CAPACITY,
            evicted.docId());
      }
    }
    parkedFailures.addLast(new ParkedFailure(
        Objects.requireNonNullElse(docId, "unknown-doc"),
        Objects.requireNonNullElse(errorType, "UnknownError"),
        Objects.requireNonNullElse(reason, "no reason"),
        Instant.now()));
  }

  public synchronized int size() {
    return parkedFailures.size();
  }

  public synchronized List<ParkedFailure> snapshot() {
    return List.copyOf(parkedFailures);
  }

  public synchronized void clear() {
    parkedFailures.clear();
  }

  public synchronized int removeByDocId(String docId) {
    Objects.requireNonNull(docId, "docId must not be null");
    int before = parkedFailures.size();
    parkedFailures.removeIf(failure -> docId.equals(failure.docId()));
    return before - parkedFailures.size();
  }

  public synchronized int removeByErrorType(String errorType) {
    Objects.requireNonNull(errorType, "errorType must not be null");
    int before = parkedFailures.size();
    parkedFailures.removeIf(failure -> errorType.equals(failure.errorType()));
    return before - parkedFailures.size();
  }

  public record ParkedFailure(String docId, String errorType, String reason, Instant parkedAt) {
  }
}
