package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;

/**
 * Host runtime boundary for resolving a logical secret reference.
 */
public interface SecretResolver {
    CompletionStage<ResolvedSecret> resolve(SecretRef reference);
}
