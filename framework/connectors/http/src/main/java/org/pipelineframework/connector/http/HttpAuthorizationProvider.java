package org.pipelineframework.connector.http;

import java.util.concurrent.CompletionStage;

/** Host callback supplying already-resolved authorization material; credential acquisition and refresh stay outside. */
@FunctionalInterface
public interface HttpAuthorizationProvider {
    CompletionStage<HttpAuthorizationMaterial> authorize(HttpAuthorizationRequest request);

    static HttpAuthorizationProvider none() {
        return request -> java.util.concurrent.CompletableFuture.completedStage(HttpAuthorizationMaterial.none());
    }
}
