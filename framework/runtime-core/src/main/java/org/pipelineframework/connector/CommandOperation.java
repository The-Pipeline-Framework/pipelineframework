package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;

/**
 * Command-family operation contract. Command outcome semantics are defined by the command runtime work.
 */
public interface CommandOperation<I, C, O> extends ConnectorOperation {
    CompletionStage<CommandOutcome<O>> dispatch(CommandInvocation<I, C> invocation);
}
