package org.pipelineframework.command;

import io.smallrye.mutiny.Uni;

/**
 * Application/provider extension point for executing a managed external command.
 */
public interface CommandConnector<I, O> {
    /**
     * Stable connector command name used by YAML command steps.
     */
    String command();

    /**
     * Executes the external command. Implementations should use the request command id as
     * the provider idempotency key or external document id where the provider supports it.
     */
    Uni<O> execute(CommandRequest<I> request);
}
