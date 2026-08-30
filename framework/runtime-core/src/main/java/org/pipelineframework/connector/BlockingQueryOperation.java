package org.pipelineframework.connector;

/**
 * Query operation whose invocation may block the calling thread.
 *
 * <p>The runtime invokes the operation on a worker and flattens the returned stage.
 */
public interface BlockingQueryOperation<I, C, O> extends QueryOperation<I, C, O>, BlockingOperation {
}
