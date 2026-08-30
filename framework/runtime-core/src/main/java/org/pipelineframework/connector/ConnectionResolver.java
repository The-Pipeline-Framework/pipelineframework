package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;

/**
 * Host runtime boundary for resolving a logical connection reference in TPF invocation context.
 */
public interface ConnectionResolver {
    <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request);
}
