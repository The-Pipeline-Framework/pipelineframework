package org.pipelineframework.connector;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Application-owned authentication, before payload mapping or completion admission. */
@FunctionalInterface
public interface ProviderCallbackAuthenticator {
    CompletionStage<Optional<ProviderCallbackActor>> authenticate(ProviderCallbackAuthenticationRequest request);
}
