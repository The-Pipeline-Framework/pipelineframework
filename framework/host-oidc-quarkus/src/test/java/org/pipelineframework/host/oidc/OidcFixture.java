package org.pipelineframework.host.oidc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Local authorization server: Quarkus remains the client under test. No live accounts. */
public class OidcFixture implements QuarkusTestResourceLifecycleManager {
    static final AtomicInteger exchanges = new AtomicInteger();
    static final AtomicInteger refreshes = new AtomicInteger();
    static final AtomicInteger graphCalls = new AtomicInteger();
    static final java.util.concurrent.atomic.AtomicReference<String> subject = new java.util.concurrent.atomic.AtomicReference<>("external-account");
    protected static final java.util.concurrent.atomic.AtomicReference<String> scope = new java.util.concurrent.atomic.AtomicReference<>("https://graph.microsoft.com/User.Read");
    static final java.util.concurrent.atomic.AtomicReference<String> tokenIssuer = new java.util.concurrent.atomic.AtomicReference<>("");
    static final java.util.concurrent.atomic.AtomicReference<String> refreshError = new java.util.concurrent.atomic.AtomicReference<>("");
    static final java.util.concurrent.atomic.AtomicBoolean rotate = new java.util.concurrent.atomic.AtomicBoolean();
    static final java.util.concurrent.atomic.AtomicBoolean loseRefreshResponse = new java.util.concurrent.atomic.AtomicBoolean();
    static final java.util.concurrent.atomic.AtomicBoolean claimsChallenge = new java.util.concurrent.atomic.AtomicBoolean();
    static final java.util.concurrent.atomic.AtomicReference<Optional<java.util.concurrent.CountDownLatch>> refreshEntered = new java.util.concurrent.atomic.AtomicReference<>(Optional.empty());
    static final java.util.concurrent.atomic.AtomicReference<Optional<java.util.concurrent.CountDownLatch>> refreshRelease = new java.util.concurrent.atomic.AtomicReference<>(Optional.empty());
    private final ObjectMapper json = new ObjectMapper();
    private final Map<String, Map<String, String>> codes = new ConcurrentHashMap<>();
    private Optional<HttpServer> server = Optional.empty();
    private final java.util.concurrent.ExecutorService requests = java.util.concurrent.Executors.newCachedThreadPool();
    private Optional<KeyPair> signingKey = Optional.empty();
    private String issuer = "";

    @Override
    public Map<String, String> start() {
        try {
            var generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            signingKey = Optional.of(generator.generateKeyPair());
            var http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server = Optional.of(http);
            issuer = "http://127.0.0.1:" + http.getAddress().getPort();
            http.createContext("/", this::handle);
            http.setExecutor(requests);
            http.start();
            return Map.ofEntries(
                Map.entry("quarkus.oidc.connection.auth-server-url", issuer),
                Map.entry("quarkus.oidc.connection.application-type", "web-app"),
                Map.entry("quarkus.oidc.connection.client-id", "test-client"),
                Map.entry("quarkus.oidc.connection.credentials.secret", "fixture-client-secret"),
                Map.entry("quarkus.oidc.connection.tenant-paths", "/authorize,/connections/proof/authorize"),
                Map.entry("quarkus.oidc.connection.provider", "microsoft"),
                Map.entry("quarkus.oidc.connection.token.issuer", issuer),
                Map.entry("quarkus.oidc.connection.authentication.pkce-required", "true"),
                Map.entry("quarkus.oidc.connection.authentication.state-secret", "test-state-secret-with-32-characters"),
                Map.entry("quarkus.oidc.connection.authentication.scopes", "openid,offline_access,https://graph.microsoft.com/User.Read"),
                Map.entry("quarkus.oidc.connection.token-state-manager.strategy", "id-token"),
                Map.entry("quarkus.oidc.connection.token.refresh-expired", "false"),
                Map.entry("quarkus.oidc-client.connection.auth-server-url", issuer),
                Map.entry("quarkus.oidc-client.connection.client-id", "test-client"),
                Map.entry("quarkus.oidc-client.connection.credentials.secret", "fixture-client-secret"),
                Map.entry("quarkus.oidc-client.connection.grant.type", "refresh"),
                Map.entry("quarkus.oidc.tenant-enabled", "false"),
                Map.entry("test.oidc.issuer", issuer));
        } catch (Exception problem) { throw new IllegalStateException("Cannot start OIDC fixture", problem); }
    }

    private void handle(HttpExchange request) {
        try {
            switch (request.getRequestURI().getPath()) {
                case "/.well-known/openid-configuration" -> reply(request, 200, Map.of(
                    "issuer", issuer, "authorization_endpoint", issuer + "/authorize", "token_endpoint", issuer + "/token",
                    "jwks_uri", issuer + "/jwks", "response_types_supported", new String[] {"code"},
                    "id_token_signing_alg_values_supported", new String[] {"RS256"},
                    "subject_types_supported", new String[] {"public"}));
                case "/jwks" -> {
                    var key = (RSAPublicKey) signingKey.orElseThrow().getPublic();
                    reply(request, 200, Map.of("keys", new Object[] {Map.of("kty", "RSA", "kid", "fixture", "use", "sig",
                        "n", unsigned(key.getModulus().toByteArray()), "e", unsigned(key.getPublicExponent().toByteArray()))}));
                }
                case "/authorize" -> {
                    var params = parameters(request.getRequestURI().getRawQuery());
                    if (!"S256".equals(params.get("code_challenge_method"))) { reply(request, 400, Map.of("error", "pkce_required")); break; }
                    String code = UUID.randomUUID().toString();
                    codes.put(code, params);
                    request.getResponseHeaders().set("Location", params.get("redirect_uri") + "?code=" + code + "&state=" + params.get("state"));
                    request.sendResponseHeaders(302, -1);
                    request.close();
                }
                case "/token" -> {
                    var params = parameters(new String(request.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                    if ("refresh_token".equals(params.get("grant_type"))) {
                        refreshes.incrementAndGet();
                        if (loseRefreshResponse.get()) { request.close(); break; }
                        refreshEntered.get().ifPresent(java.util.concurrent.CountDownLatch::countDown);
                        if (refreshRelease.get().isPresent()) { refreshRelease.get().orElseThrow().await(10, java.util.concurrent.TimeUnit.SECONDS); }
                        if (!refreshError.get().isEmpty()) { reply(request, 400, Map.of("error", refreshError.get())); break; }
                        var renewed = new HashMap<String, Object>(Map.of("access_token", "renewed-access", "expires_in", 3600,
                            "token_type", "Bearer", "scope", scope.get()));
                        if (rotate.get()) { renewed.put("refresh_token", "rotated-refresh"); }
                        reply(request, 200, renewed);
                        break;
                    }
                    var authorization = Optional.ofNullable(codes.remove(params.getOrDefault("code", "")));
                    String digest = encode(MessageDigest.getInstance("SHA-256").digest(params.getOrDefault("code_verifier", "").getBytes(StandardCharsets.US_ASCII)));
                    if (authorization.isEmpty() || !digest.equals(authorization.orElseThrow().get("code_challenge"))) {
                        reply(request, 400, Map.of("error", "invalid_grant")); break;
                    }
                    exchanges.incrementAndGet();
                    var claims = new HashMap<String, Object>(Map.of("iss", tokenIssuer.get().isEmpty() ? issuer : tokenIssuer.get(), "sub", subject.get(), "name", "Fixture User",
                        "aud", "test-client", "iat", Instant.now().getEpochSecond(), "exp", Instant.now().plusSeconds(3600).getEpochSecond()));
                    Optional.ofNullable(authorization.orElseThrow().get("nonce")).ifPresent(nonce -> claims.put("nonce", nonce));
                    reply(request, 200, Map.of("id_token", jwt(claims), "access_token", "initial-access",
                        "refresh_token", "fixture-refresh", "expires_in", 3600, "token_type", "Bearer", "scope", scope.get()));
                }
                case "/v1.0/me" -> {
                    graphCalls.incrementAndGet();
                    if (claimsChallenge.get()) {
                        request.getResponseHeaders().set("WWW-Authenticate", "Bearer error=\"insufficient_claims\", claims=\"fixture\"");
                        reply(request, 401, Map.of("error", "interaction_required"));
                    } else if (Set.of("Bearer initial-access", "Bearer renewed-access").contains(
                        Optional.ofNullable(request.getRequestHeaders().getFirst("Authorization")).orElse(""))) {
                        reply(request, 200, Map.of("id", "fixture-user", "displayName", "Fixture User"));
                    } else { reply(request, 401, Map.of("error", "missing_authorization")); }
                }
                default -> reply(request, 404, Map.of());
            }
        } catch (Exception problem) {
            try { reply(request, 500, Map.of("error", "fixture_failure")); }
            catch (Exception ignored) { request.close(); }
        }
    }

    private String jwt(Map<String, Object> claims) throws Exception {
        String body = encode(json.writeValueAsBytes(Map.of("alg", "RS256", "kid", "fixture"))) + "." + encode(json.writeValueAsBytes(claims));
        var signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signingKey.orElseThrow().getPrivate());
        signature.update(body.getBytes(StandardCharsets.US_ASCII));
        return body + "." + encode(signature.sign());
    }

    private void reply(HttpExchange exchange, int status, Object body) throws Exception {
        byte[] bytes = json.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }

    private Map<String, String> parameters(String query) {
        Map<String, String> result = new HashMap<>();
        for (String pair : Optional.ofNullable(query).orElse("").split("&")) {
            String[] parts = pair.split("=", 2);
            if (parts.length == 2) { result.put(decode(parts[0]), decode(parts[1])); }
        }
        return result;
    }
    private String decode(String value) { return URLDecoder.decode(value, StandardCharsets.UTF_8); }
    private String unsigned(byte[] bytes) { return encode(bytes[0] == 0 ? java.util.Arrays.copyOfRange(bytes, 1, bytes.length) : bytes); }
    private String encode(byte[] bytes) { return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes); }
    @Override public void stop() { server.ifPresent(http -> http.stop(0)); requests.shutdownNow(); }
}
