package org.pipelineframework.awaitable;

import java.util.List;
import java.util.Map;
import io.smallrye.mutiny.Uni;

/** Application-owned verification of the raw provider request before completion admission. */
public interface ProviderCallbackAuthenticator {
    Uni<Boolean> authenticate(AwaitInteractionRecord interaction, String method,
        Map<String, List<String>> headers, byte[] body);
}
