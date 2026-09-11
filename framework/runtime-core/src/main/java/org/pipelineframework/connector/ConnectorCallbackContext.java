package org.pipelineframework.connector;

import java.net.URI;
import java.util.Objects;

/** Transient framework-supplied callback authority. Never include it in persisted effect evidence. */
public record ConnectorCallbackContext(String callbackId, URI callbackUri, UriPolicy uriPolicy) {
    public enum UriPolicy {
        HTTPS_ONLY,
        /** Explicit host opt-in for local deployments, including container-network hostnames. */
        LOCAL_HTTP
    }

    public ConnectorCallbackContext(String callbackId, URI callbackUri) {
        this(callbackId, callbackUri, UriPolicy.HTTPS_ONLY);
    }

    public ConnectorCallbackContext {
        callbackId = ConnectorProviderId.require(callbackId, "callback ID");
        Objects.requireNonNull(callbackUri, "callback URI must not be null");
        Objects.requireNonNull(uriPolicy, "callback URI policy must not be null");
        boolean secure = "https".equalsIgnoreCase(callbackUri.getScheme());
        boolean allowPlaintext = uriPolicy == UriPolicy.LOCAL_HTTP && "http".equalsIgnoreCase(callbackUri.getScheme());
        if ((!secure && !allowPlaintext) || callbackUri.getHost() == null || callbackUri.getUserInfo() != null
            || callbackUri.getFragment() != null) {
            throw new IllegalArgumentException("callback URI must be an absolute HTTPS endpoint without user info or fragment"
                + " (HTTP requires explicit local policy)");
        }
    }

    @Override
    public String toString() {
        return "ConnectorCallbackContext[callbackId=" + callbackId + ", callbackUri=<redacted>]";
    }
}
