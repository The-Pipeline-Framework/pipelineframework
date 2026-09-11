package org.pipelineframework.awaitable;

import java.net.URI;

/** Application-owned address resolution. The signed token must not be logged or persisted. */
public interface ProviderCallbackEndpointResolver {
    URI resolve(AwaitInteractionRecord interaction, String signedResumeToken);
}
