package org.pipelineframework.host.microsoft.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.host.oidc.ConnectionFailure;
import org.pipelineframework.host.oidc.ConnectionRegistration;

/** Runtime-only, host-authenticated Microsoft Graph capability with a bounded {@code /me} operation. */
public final class AuthenticatedMicrosoftGraphConnection implements ResolvedConnection {
    private static final int MAX_PROFILE_BYTES = 64 * 1024;
    private final ConnectionRegistration.RequestAccess access;
    private final HttpClient http;
    private final URI meEndpoint;
    private final ObjectMapper json;
    private final Duration timeout;

    AuthenticatedMicrosoftGraphConnection(ConnectionRegistration.RequestAccess access, HttpClient http,
            URI meEndpoint, ObjectMapper json, Duration timeout) {
        this.access = access;
        this.http = http;
        this.meEndpoint = meEndpoint;
        this.json = json;
        this.timeout = timeout;
    }

    /** Blocking bounded profile read. Call from the Connector's blocking executor. */
    public MicrosoftGraphProfile me() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(meEndpoint)
            .header("Authorization", "Bearer " + access.accessToken())
            .header("Accept", "application/json")
            .timeout(timeout)
            .GET()
            .build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var body = response.body()) {
            if (response.statusCode() == 401) {
                access.requiresInteraction();
                throw new ConnectionFailure(ConnectionFailure.Reason.REAUTHORIZE);
            }
            if (response.statusCode() != 200) {
                throw new MicrosoftGraphRequestException(response.statusCode(), "Microsoft Graph profile request failed");
            }
            byte[] payload = body.readNBytes(MAX_PROFILE_BYTES + 1);
            if (payload.length > MAX_PROFILE_BYTES) {
                throw new MicrosoftGraphRequestException(200, "Microsoft Graph profile exceeded the response limit");
            }
            JsonNode profile;
            try {
                profile = json.readTree(payload);
            } catch (IOException problem) {
                throw new MicrosoftGraphRequestException(200, "Microsoft Graph returned an invalid profile");
            }
            if (profile == null || !profile.isObject()) {
                throw new MicrosoftGraphRequestException(200, "Microsoft Graph returned an invalid profile");
            }
            var id = text(profile, "id").orElseThrow(
                () -> new MicrosoftGraphRequestException(200, "Microsoft Graph returned a profile without an id"));
            return new MicrosoftGraphProfile(id, text(profile, "displayName"),
                text(profile, "userPrincipalName"), text(profile, "mail"));
        }
    }

    private static Optional<String> text(JsonNode source, String field) {
        return Optional.ofNullable(source.get(field))
            .filter(JsonNode::isTextual)
            .map(JsonNode::textValue)
            .filter(value -> !value.isBlank());
    }
}
