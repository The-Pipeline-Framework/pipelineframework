package org.pipelineframework.host.microsoft.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import org.pipelineframework.host.oidc.ConnectionRegistration;

/** Microsoft Graph client construction only. Quarkus owns authorization and credential renewal. */
public final class MicrosoftGraphClients implements ConnectionRegistration.ClientFactory<AuthenticatedMicrosoftGraphConnection> {
    public static final String USER_READ_SCOPE = "https://graph.microsoft.com/User.Read";
    public static final URI PUBLIC_CLOUD_ORIGIN = URI.create("https://graph.microsoft.com");
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
    private final HttpClient http;
    private final URI meEndpoint;
    private final ObjectMapper json;
    private final Duration timeout;

    public MicrosoftGraphClients(HttpClient http, ObjectMapper json) {
        this(http, PUBLIC_CLOUD_ORIGIN, json, DEFAULT_TIMEOUT);
    }

    /** Supports host-selected sovereign-cloud origins and HTTP loopback fixtures. */
    public MicrosoftGraphClients(HttpClient http, URI graphOrigin, ObjectMapper json, Duration timeout) {
        this.http = Objects.requireNonNull(http);
        this.json = Objects.requireNonNull(json);
        this.timeout = Objects.requireNonNull(timeout);
        Objects.requireNonNull(graphOrigin);
        if (http.followRedirects() != HttpClient.Redirect.NEVER || timeout.isZero() || timeout.isNegative()
            || !validOrigin(graphOrigin)) {
            throw new IllegalArgumentException("Invalid Microsoft Graph client configuration");
        }
        meEndpoint = graphOrigin.resolve("/v1.0/me");
    }

    @Override
    public AuthenticatedMicrosoftGraphConnection create(ConnectionRegistration.RequestAccess access) {
        return new AuthenticatedMicrosoftGraphConnection(Objects.requireNonNull(access), http, meEndpoint, json, timeout);
    }

    private boolean validOrigin(URI origin) {
        if (!origin.isAbsolute() || origin.isOpaque() || origin.getHost() == null
            || origin.getPath() == null || origin.getUserInfo() != null) {
            return false;
        }
        boolean loopback = "http".equals(origin.getScheme())
            && Set.of("localhost", "127.0.0.1", "[::1]", "::1").contains(origin.getHost());
        return ("https".equals(origin.getScheme()) || loopback)
            && (origin.getPath().isEmpty() || "/".equals(origin.getPath()))
            && origin.getRawQuery() == null && origin.getRawFragment() == null;
    }
}
