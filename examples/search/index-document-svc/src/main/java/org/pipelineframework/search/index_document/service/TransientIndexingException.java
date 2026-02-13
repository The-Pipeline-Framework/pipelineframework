package org.pipelineframework.search.index_document.service;

/**
 * Signals a transient indexing failure that may succeed on retry.
 */
public final class TransientIndexingException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public TransientIndexingException(String message) {
    super(message);
  }

  public TransientIndexingException(Throwable cause) {
    super(cause);
  }

  public TransientIndexingException(String message, Throwable cause) {
    super(message, cause);
  }
}
