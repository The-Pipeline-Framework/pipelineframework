package org.pipelineframework.host.gmail;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.auth.oauth2.GoogleRefreshTokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.gmail.Gmail;
import org.pipelineframework.connector.gmail.GmailQueryConnector;

/** Google protocol operations delegated to Google's maintained Java client library. */
public final class GoogleGmailAuthorization implements GmailAuthorization {
    private static final String READ_SCOPE = GmailQueryConnector.REQUIRED_OAUTH_SCOPE;
    private final String clientId;
    private final String clientSecret;
    private final URI callback;
    private final HttpTransport transport;
    private final JsonFactory json;
    private final Clock clock;

    public GoogleGmailAuthorization(String clientId, String clientSecret, URI callback,
                                    HttpTransport transport, JsonFactory json, Clock clock) {
        this.clientId = Objects.requireNonNull(clientId);
        this.clientSecret = Objects.requireNonNull(clientSecret);
        this.callback = Objects.requireNonNull(callback);
        this.transport = Objects.requireNonNull(transport);
        this.json = Objects.requireNonNull(json);
        this.clock = Objects.requireNonNull(clock);
        if (clientId.isBlank() || clientSecret.isBlank() || !"https".equals(callback.getScheme())
            || callback.getHost() == null || callback.getRawQuery() != null || callback.getRawFragment() != null) {
            throw new IllegalArgumentException("Google registration requires an HTTPS callback without query or fragment");
        }
    }

    @Override
    public String registrationId() { return clientId; }

    @Override
    public URI authorize(String state, String verifier) {
        try {
            String challenge = Base64.getUrlEncoder().withoutPadding().encodeToString(
                MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII)));
            var url = new GoogleAuthorizationCodeRequestUrl(clientId, callback.toString(), List.of(READ_SCOPE, "openid"))
                .setAccessType("offline").setState(state);
            url.set("prompt", "consent");
            url.set("code_challenge", challenge);
            url.set("code_challenge_method", "S256");
            return URI.create(url.build());
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }

    @Override
    public Grant exchange(String code, String verifier, Optional<Grant> previous) {
        try {
            var request = new GoogleAuthorizationCodeTokenRequest(transport, json, clientId, clientSecret,
                code, callback.toString());
            request.set("code_verifier", verifier);
            request.setRequestInitializer(this::configureRequest);
            var response = request.execute();
            String idToken = Optional.ofNullable(response.getIdToken()).orElseThrow(this::reauthorize);
            var verified = Optional.ofNullable(new GoogleIdTokenVerifier.Builder(transport, json)
                .setAudience(List.of(clientId)).setClock(clock::millis).build().verify(idToken))
                .orElseThrow(this::reauthorize);
            String subject = verified.getPayload().getSubject();
            Optional<Grant> sameAccount = previous.filter(grant -> grant.subject().equals(subject));
            return grant(response, subject, sameAccount, false);
        } catch (TokenResponseException failure) {
            throw tokenFailure(failure);
        } catch (IOException | java.security.GeneralSecurityException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.UNAVAILABLE);
        }
    }

    @Override
    public Grant refresh(Grant previous) {
        try {
            var request = new GoogleRefreshTokenRequest(transport, json, previous.refreshToken(), clientId, clientSecret);
            request.setRequestInitializer(this::configureRequest);
            return grant(request.execute(), previous.subject(), Optional.of(previous), true);
        } catch (TokenResponseException failure) {
            throw tokenFailure(failure);
        } catch (IOException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.UNAVAILABLE);
        }
    }

    private Grant grant(TokenResponse response, String subject, Optional<Grant> previous, boolean refresh) {
        String refreshToken = Optional.ofNullable(response.getRefreshToken()).filter(value -> !value.isBlank())
            .or(() -> previous.map(Grant::refreshToken)).orElseThrow(this::reauthorize);
        Set<String> scopes = Optional.ofNullable(response.getScope())
            .map(value -> Set.copyOf(List.of(value.trim().split("\\s+"))))
            .orElseGet(() -> refresh ? previous.orElseThrow().scopes() : Set.of());
        if (!scopes.contains(READ_SCOPE)) { throw reauthorize(); }
        long expires = Optional.ofNullable(response.getExpiresInSeconds()).orElseThrow(this::reauthorize);
        if (expires <= 60 || expires > 86_400) { throw reauthorize(); }
        String accessToken = Optional.ofNullable(response.getAccessToken()).filter(value -> !value.isBlank())
            .orElseThrow(this::reauthorize);
        return new Grant(subject, accessToken, refreshToken, clock.millis() + expires * 1000, scopes);
    }

    private ConnectionFailure tokenFailure(TokenResponseException failure) {
        String error = Optional.ofNullable(failure.getDetails()).map(details -> details.getError()).orElse("");
        if ("invalid_grant".equals(error)) { return reauthorize(); }
        if ("temporarily_unavailable".equals(error) || failure.getStatusCode() == 429) {
            return new ConnectionFailure(ConnectionFailure.Reason.RETRY_LATER);
        }
        return new ConnectionFailure(ConnectionFailure.Reason.UNAVAILABLE);
    }

    private ConnectionFailure reauthorize() { return new ConnectionFailure(ConnectionFailure.Reason.REAUTHORIZE); }

    @Override
    public Gmail client(Grant grant, Runnable beforeRequest) {
        return new Gmail.Builder(transport, json, request -> {
            beforeRequest.run();
            request.getHeaders().setAuthorization("Bearer " + grant.accessToken());
            configureRequest(request);
        }).setApplicationName("tpf-host-gmail").build();
    }

    @Override
    public void close() {
        try { transport.shutdown(); } catch (IOException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.UNAVAILABLE);
        }
    }

    private void configureRequest(com.google.api.client.http.HttpRequest request) {
        request.setConnectTimeout(10_000);
        request.setReadTimeout(10_000);
        request.setNumberOfRetries(0);
        request.setLoggingEnabled(false);
        request.setCurlLoggingEnabled(false);
    }
}
