package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;

/**
 * Legacy host boundary for resolving a context-free secret reference.
 *
 * @deprecated This resolver receives no connector invocation or tenant context and must not be
 * used to authenticate external connector access. Use {@link ConnectionResolver} with a typed
 * {@link ConnectionResolutionRequest} instead.
 */
@Deprecated(forRemoval = true)
public interface SecretResolver {
    CompletionStage<ResolvedSecret> resolve(SecretRef reference);
}
