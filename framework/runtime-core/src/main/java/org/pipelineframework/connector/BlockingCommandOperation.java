package org.pipelineframework.connector;

/**
 * Command operation whose invocation may block the calling thread.
 *
 * <p>The runtime invokes the operation on a worker and flattens the returned stage.
 */
public interface BlockingCommandOperation<I, C, O> extends CommandOperation<I, C, O>, BlockingOperation {
}
