package org.pipelineframework.connector.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Objects;
import java.util.Set;

import org.pipelineframework.connector.ResolvedConnection;

/** Borrowed asynchronous client, application-selected authority, and host-owned authorization. */
public final class HttpClientConnection implements ResolvedConnection {
    private final HttpClient client;
    private final URI baseUri;
    private final Set<String> securityCapabilities;
    private final HttpAuthorizationProvider authorizationProvider;

    public HttpClientConnection(
        HttpClient client,
        URI baseUri,
        Set<String> securityCapabilities,
        HttpAuthorizationProvider authorizationProvider
    ) {
        this.client = Objects.requireNonNull(client, "HTTP client must not be null");
        if (client.followRedirects() != HttpClient.Redirect.NEVER) {
            throw new IllegalArgumentException(
                "host HTTP client must disable automatic redirects so pinned origin authority cannot drift");
        }
        this.baseUri = baseUri(baseUri);
        this.securityCapabilities = Set.copyOf(Objects.requireNonNull(securityCapabilities,
            "HTTP security capabilities must not be null"));
        if (this.securityCapabilities.stream().anyMatch(value -> value == null || value.isBlank())) {
            throw new IllegalArgumentException("HTTP security capabilities must be non-blank");
        }
        this.authorizationProvider = Objects.requireNonNull(authorizationProvider,
            "HTTP authorization provider must not be null");
    }

    public HttpClient client() {
        return client;
    }

    public URI baseUri() {
        return baseUri;
    }

    public Set<String> securityCapabilities() {
        return securityCapabilities;
    }

    public HttpAuthorizationProvider authorizationProvider() {
        return authorizationProvider;
    }

    private static URI baseUri(URI value) {
        URI uri = Objects.requireNonNull(value, "HTTP base URI must not be null").normalize();
        if (!uri.isAbsolute() || uri.getHost() == null
            || !("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
            || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("HTTP base URI must be an absolute HTTP(S) authority without credentials, query, or fragment");
        }
        return uri;
    }
}
