package org.pipelineframework.host.gmail;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.gmail.GmailQueryConnector;

class GoogleGmailAuthorizationTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-09-05T00:00:00Z"), ZoneOffset.UTC);
    private final String scope = GmailQueryConnector.REQUIRED_OAUTH_SCOPE;

    @Test
    void authorizationRequestsOfflineAccessStateAndS256() {
        try (var provider = provider(200, "{}", new AtomicReference<>())) {
            String url = provider.authorize("opaque-state", "verifier").toString();
            assertTrue(url.startsWith("https://accounts.google.com/"));
            assertTrue(url.contains("access_type=offline"));
            assertTrue(url.contains("state=opaque-state"));
            assertTrue(url.contains("code_challenge_method=S256"));
            assertFalse(url.contains("client-secret"));
            assertFalse(url.contains("verifier"));
        }
    }

    @Test
    void refreshPreservesOmittedRefreshTokenAndScopesAndSendsCorrectGrant() {
        var body = new AtomicReference<String>();
        try (var provider = provider(200, "{\"access_token\":\"new-access\",\"expires_in\":3600,\"token_type\":\"Bearer\"}", body)) {
            var renewed = provider.refresh(grant());
            assertEquals("refresh-secret", renewed.refreshToken());
            assertEquals(Set.of(scope), renewed.scopes());
            assertEquals("subject", renewed.subject());
            assertEquals(clock.millis() + 3_600_000, renewed.expiresAt());
            assertTrue(body.get().contains("grant_type=refresh_token"));
            assertTrue(body.get().contains("refresh_token=refresh-secret"));
            assertFalse(renewed.toString().contains("secret"));
        }
    }

    @Test
    void refreshPersistsReplacementAndRejectsLostRequiredScope() {
        try (var provider = provider(200,
            "{\"access_token\":\"new-access\",\"refresh_token\":\"new-refresh\",\"expires_in\":3600,\"scope\":\"" + scope + "\"}",
            new AtomicReference<>())) {
            assertEquals("new-refresh", provider.refresh(grant()).refreshToken());
        }
        try (var provider = provider(200,
            "{\"access_token\":\"new-access\",\"expires_in\":3600,\"scope\":\"openid\"}", new AtomicReference<>())) {
            assertEquals(ConnectionFailure.Reason.REAUTHORIZE,
                assertThrows(ConnectionFailure.class, () -> provider.refresh(grant())).reason());
        }
    }

    @Test
    void errorsAreBoundedAndDefiniteRateLimitsDifferFromUnknownOutcomes() {
        try (var provider = provider(400, "{\"error\":\"invalid_grant\",\"error_description\":\"refresh-secret\"}", new AtomicReference<>())) {
            var failure = assertThrows(ConnectionFailure.class, () -> provider.refresh(grant()));
            assertEquals(ConnectionFailure.Reason.REAUTHORIZE, failure.reason());
            assertFalse(failure.toString().contains("secret"));
            assertTrue(Optional.ofNullable(failure.getCause()).isEmpty());
        }
        try (var provider = provider(429, "{\"error\":\"temporarily_unavailable\"}", new AtomicReference<>())) {
            assertEquals(ConnectionFailure.Reason.RETRY_LATER,
                assertThrows(ConnectionFailure.class, () -> provider.refresh(grant())).reason());
        }
        try (var provider = provider(503, "{}", new AtomicReference<>())) {
            assertEquals(ConnectionFailure.Reason.UNAVAILABLE,
                assertThrows(ConnectionFailure.class, () -> provider.refresh(grant())).reason());
        }
    }

    @Test
    void missingAccessTokenIsRejectedAndSdkCredentialLoggingIsDisabled() throws Exception {
        try (var provider = provider(200, "{\"expires_in\":3600}", new AtomicReference<>())) {
            assertEquals(ConnectionFailure.Reason.REAUTHORIZE,
                assertThrows(ConnectionFailure.class, () -> provider.refresh(grant())).reason());
            var request = provider.client(grant(), () -> { }).users().messages().list("me").buildHttpRequest();
            assertFalse(request.isLoggingEnabled());
            assertFalse(request.isCurlLoggingEnabled());
            assertEquals(0, request.getNumberOfRetries());
        }
    }

    @Test
    void codeExchangeIncludesPkceAndRequiresVerifiedAccountIdentity() {
        var body = new AtomicReference<String>();
        try (var provider = provider(200, "{\"access_token\":\"access\",\"refresh_token\":\"refresh\",\"expires_in\":3600}", body)) {
            assertEquals(ConnectionFailure.Reason.REAUTHORIZE,
                assertThrows(ConnectionFailure.class, () -> provider.exchange("code", "verifier", Optional.empty())).reason());
            assertTrue(body.get().contains("code_verifier=verifier"));
            assertTrue(body.get().contains("grant_type=authorization_code"));
        }
    }

    private GmailAuthorization.Grant grant() {
        return new GmailAuthorization.Grant("subject", "access-secret", "refresh-secret", clock.millis(), Set.of(scope));
    }

    private GoogleGmailAuthorization provider(int status, String response, AtomicReference<String> body) {
        var transport = new MockHttpTransport() {
            @Override public MockLowLevelHttpRequest buildRequest(String method, String url) {
                return new MockLowLevelHttpRequest(url) {
                    @Override public MockLowLevelHttpResponse execute() throws IOException {
                        body.set(getContentAsString());
                        return new MockLowLevelHttpResponse().setStatusCode(status).setContentType("application/json").setContent(response);
                    }
                };
            }
        };
        return new GoogleGmailAuthorization("client-id", "client-secret", URI.create("https://host.example/gmail/callback"),
            transport, GsonFactory.getDefaultInstance(), clock);
    }
}
