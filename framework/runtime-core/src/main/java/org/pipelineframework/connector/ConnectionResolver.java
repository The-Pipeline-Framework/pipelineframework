package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;

/**
 * Host runtime boundary for resolving a logical connection reference.
 */
public interface ConnectionResolver {
    CompletionStage<ResolvedConnection> resolve(ConnectionRef reference);
}
