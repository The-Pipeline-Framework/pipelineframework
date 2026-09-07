package org.pipelineframework.host.oidc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.quarkus.oidc.AuthorizationCodeTokens;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.OidcClientException;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ResolvedConnection;

/** Optional Quarkus host bridge. OAuth protocol operations belong exclusively to Quarkus. */
public final class QuarkusConnections implements AutoCloseable {
    public enum Phase { DISCONNECTED, CONNECTING, READY, REFRESHING, RENEWAL_UNCERTAIN, REQUIRES_REAUTHORIZATION }
    public record Status(Phase phase, long revision) { }
    private record Cached(long generation, ResolvedConnection connection) { }
    private final JdbcConnectionStore store;
    private final ConnectionRegistration<?> registration;
    private final OidcClient client;
    private final Executor executor;
    private final Clock clock;
    private final ObjectMapper json = new ObjectMapper().registerModule(new Jdk8Module());
    private final Map<ConnectionKey, Cached> clients = new LinkedHashMap<>(16, 0.75f, true);
    private final AtomicBoolean closed = new AtomicBoolean();

    public QuarkusConnections(JdbcConnectionStore store, ConnectionRegistration<?> registration,
                              OidcClient client, Executor blockingExecutor, Clock clock) {
        this.store = Objects.requireNonNull(store);
        this.registration = Objects.requireNonNull(registration);
        this.client = Objects.requireNonNull(client);
        this.executor = Objects.requireNonNull(blockingExecutor);
        this.clock = Objects.requireNonNull(clock);
        if (!registration.storageId().equals(store.registrationId())) {
            throw new IllegalArgumentException("Store and OIDC registration must match");
        }
        var configuration = org.eclipse.microprofile.config.ConfigProvider.getConfig();
        String prefix = "quarkus.oidc-client." + registration.tenantId() + ".";
        if (!configuration.getOptionalValue(prefix + "client-id", String.class).orElse("").equals(registration.clientId())) {
            throw new IllegalArgumentException("Use the matching named OidcClient");
        }
    }

    ConnectionRegistration<?> registration() { return registration; }

    /** Call only after host management authorization. The result is a protected HttpOnly browser ticket. */
    public CompletionStage<String> begin(ConnectionKey key, String actor) {
        return offload(() -> {
            if (actor == null || actor.isBlank()) { throw failure(ConnectionFailure.Reason.FORBIDDEN); }
            ConnectionState previous = current(key);
            String browser = store.encryption().nonce();
            long expiry = now() + 600_000;
            var challenge = new ConnectionState.Challenge(store.encryption().hash(browser), actor, expiry);
            var pending = previous.replace(Phase.CONNECTING, now(), Optional.empty(), Optional.of(challenge));
            save(key, pending);
            return encode(new ConnectionAttempt(key, actor, browser, pending.revision(), expiry, Optional.empty()));
        });
    }

    /** CPU-only redirect hook: binds our attempt to the state Quarkus has already generated. */
    String bind(String ticket, String state) {
        var attempt = decode(ticket);
        if (state == null || state.isBlank()) { throw failure(ConnectionFailure.Reason.INVALID_CALLBACK); }
        return encode(attempt.bind(store.encryption().hash(state)));
    }

    ConnectionAttempt decode(String ticket) {
        try {
            if (ticket == null || ticket.length() > 16000) { throw failure(ConnectionFailure.Reason.INVALID_CALLBACK); }
            var attempt = json.readValue(store.encryption().decrypt("oidc-attempt:" + registration.storageId(), ticket), ConnectionAttempt.class);
            if (attempt.expiresAt() <= now()) { throw failure(ConnectionFailure.Reason.INVALID_CALLBACK); }
            return attempt;
        } catch (java.io.IOException | RuntimeException problem) {
            throw failure(ConnectionFailure.Reason.INVALID_CALLBACK);
        }
    }

    private String encode(ConnectionAttempt attempt) {
        try { return store.encryption().encrypt("oidc-attempt:" + registration.storageId(), json.writeValueAsBytes(attempt)); }
        catch (java.io.IOException problem) { throw failure(ConnectionFailure.Reason.STORAGE); }
    }

    /** Package-private: called by the Quarkus completion action only after token verification. */
    CompletionStage<Status> complete(ConnectionAttempt attempt, String state, JsonWebToken identity, AuthorizationCodeTokens tokens) {
        return offload(() -> {
            var pending = current(attempt.key());
            var challenge = pending.challenge().orElseThrow(() -> failure(ConnectionFailure.Reason.INVALID_CALLBACK));
            if (pending.phase() != Phase.CONNECTING || pending.revision() != attempt.revision()
                || challenge.expiresAt() <= now() || !challenge.actor().equals(attempt.actor())
                || !challenge.browserHash().equals(store.encryption().hash(attempt.browserSecret()))
                || state == null || !attempt.stateHash().equals(Optional.of(store.encryption().hash(state)))) {
                throw failure(ConnectionFailure.Reason.INVALID_CALLBACK);
            }
            if (!registration.issuer().toString().equals(identity.getIssuer()) || !registration.accountPolicy().test(identity)) {
                throw failure(ConnectionFailure.Reason.FORBIDDEN);
            }
            String subject = Optional.ofNullable(identity.getSubject()).filter(value -> !value.isBlank())
                .orElseThrow(() -> failure(ConnectionFailure.Reason.FORBIDDEN));
            String account = identity.getIssuer().length() + ":" + identity.getIssuer() + subject;
            var grantedScopes = Optional.ofNullable(tokens.getAccessTokenScope()).filter(value -> !value.isBlank())
                .map(this::scopes).orElseGet(registration::requiredScopes);
            var grant = new ConnectionState.Grant(account, required(tokens.getAccessToken()), required(tokens.getRefreshToken()),
                expires(tokens.getAccessTokenExpiresIn()), grantedScopes);
            requireScopes(grant.scopes());
            store.claimAccount(attempt.key(), account);
            var ready = pending.next(Phase.READY, now(), Optional.of(grant), Optional.empty());
            save(attempt.key(), ready);
            return status(ready);
        });
    }

    CompletionStage<Boolean> completed(String ticket) {
        var attempt = decode(ticket);
        return offload(() -> {
            var current = current(attempt.key());
            return current.phase() == Phase.READY && current.revision() == attempt.revision() + 1;
        });
    }

    public CompletionStage<Status> status(ConnectionKey key) { return offload(() -> status(current(key))); }

    /** Host scheduler entry point for an authorized logical key; uses the same durable claim as live resolution. */
    public CompletionStage<Status> renew(ConnectionKey key) { return offload(() -> status(usable(key))); }

    public CompletionStage<Status> disconnect(ConnectionKey key) {
        return offload(() -> {
            for (int i = 0; i < 8; i++) {
                var previous = current(key);
                var disconnected = previous.replace(Phase.DISCONNECTED, now(), Optional.empty(), Optional.empty());
                if (store.append(key, disconnected)) { invalidate(key); return status(disconnected); }
            }
            throw failure(ConnectionFailure.Reason.CONFLICT);
        });
    }

    /** Explicit host resolver delegation; installs no CDI resolver and never selects authority from operation data. */
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        return offload(() -> {
            if (request.connectionType() != registration.connectionType()) { throw failure(ConnectionFailure.Reason.FORBIDDEN); }
            var key = new ConnectionKey(request.invocationContext().tenantId()
                .orElseThrow(() -> failure(ConnectionFailure.Reason.FORBIDDEN)), request.reference());
            var state = usable(key);
            synchronized (clients) {
                var cached = clients.get(key);
                if (cached == null || cached.generation() != state.generation()) {
                    var capability = registration.factory().create(new ConnectionRegistration.RequestAccess() {
                        @Override public String accessToken() {
                            if (current(key).generation() != state.generation()) { throw failure(ConnectionFailure.Reason.UNAVAILABLE); }
                            var actual = usable(key);
                            if (actual.generation() != state.generation()) { throw failure(ConnectionFailure.Reason.UNAVAILABLE); }
                            return actual.grant().orElseThrow().accessToken();
                        }
                        @Override public void requiresInteraction() {
                            var actual = current(key);
                            if (actual.generation() == state.generation()) {
                                save(key, actual.next(Phase.REQUIRES_REAUTHORIZATION, now(), Optional.empty(), Optional.empty()));
                            }
                        }
                    });
                    cached = new Cached(state.generation(), capability);
                    clients.put(key, cached);
                    if (clients.size() > 128) { clients.remove(clients.keySet().iterator().next()); }
                }
                return request.connectionType().cast(cached.connection());
            }
        }).exceptionallyCompose(problem -> {
            var cause = unwrap(problem);
            var reason = cause instanceof ConnectionFailure failure ? failure.reason() : ConnectionFailure.Reason.UNAVAILABLE;
            var kind = switch (reason) {
                case REAUTHORIZE -> ConnectionResolutionException.Kind.AUTHENTICATION_REQUIRED;
                case FORBIDDEN, INVALID_CALLBACK -> ConnectionResolutionException.Kind.CONFIGURATION;
                default -> ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE;
            };
            return CompletableFuture.failedStage(new ConnectionResolutionException(kind, "oidc-connection-" + reason.name()));
        });
    }

    private ConnectionState usable(ConnectionKey key) {
        for (int i = 0; i < 100; i++) {
            var state = current(key);
            if (state.phase() == Phase.REFRESHING) {
                try { Thread.sleep(20); } catch (InterruptedException problem) {
                    Thread.currentThread().interrupt(); throw failure(ConnectionFailure.Reason.UNAVAILABLE);
                }
                continue;
            }
            if (state.phase() == Phase.RENEWAL_UNCERTAIN) { throw failure(ConnectionFailure.Reason.UNCERTAIN); }
            if (state.phase() != Phase.READY) { throw failure(ConnectionFailure.Reason.REAUTHORIZE); }
            var previous = state.grant().orElseThrow();
            if (previous.expiresAt() > now() + 30_000) { return state; }
            var claimed = state.next(Phase.REFRESHING, now(), state.grant(), Optional.empty());
            if (!store.append(key, claimed)) { continue; }
            try {
                var tokens = client.refreshTokens(previous.refreshToken()).await().atMost(Duration.ofSeconds(20));
                var expiry = Optional.ofNullable(tokens.getAccessTokenExpiresAt()).orElseThrow(() -> failure(ConnectionFailure.Reason.UNCERTAIN));
                var grant = new ConnectionState.Grant(previous.account(), required(tokens.getAccessToken()),
                    Optional.ofNullable(tokens.getRefreshToken()).filter(value -> !value.isBlank()).orElse(previous.refreshToken()),
                    Math.multiplyExact(expiry, 1000), Optional.ofNullable(tokens.get("scope")).map(this::scopes).orElse(previous.scopes()));
                requireScopes(grant.scopes());
                if (grant.expiresAt() <= now()) { throw failure(ConnectionFailure.Reason.REAUTHORIZE); }
                var ready = claimed.next(Phase.READY, now(), Optional.of(grant), Optional.empty());
                save(key, ready);
                return ready;
            } catch (RuntimeException problem) {
                var reason = renewalFailure(problem);
                var next = switch (reason) {
                    case REAUTHORIZE -> claimed.next(Phase.REQUIRES_REAUTHORIZATION, now(), Optional.empty(), Optional.empty());
                    case RETRY_LATER -> claimed.next(Phase.READY, now(), state.grant(), Optional.empty());
                    default -> claimed.next(Phase.RENEWAL_UNCERTAIN, now(), state.grant(), Optional.empty());
                };
                // Conditional append fences late results after disconnect/reconnect or another recovery action.
                store.append(key, next);
                throw failure(reason);
            }
        }
        throw failure(ConnectionFailure.Reason.RETRY_LATER);
    }

    private ConnectionFailure.Reason renewalFailure(RuntimeException problem) {
        var cause = unwrap(problem);
        if (cause instanceof ConnectionFailure failure) { return failure.reason(); }
        if (cause instanceof OidcClientException) {
            try {
                String code = json.readTree(Optional.ofNullable(cause.getMessage()).orElse("{}")).path("error").asText();
                if (Set.of("invalid_grant", "interaction_required", "consent_required", "login_required").contains(code)) {
                    return ConnectionFailure.Reason.REAUTHORIZE;
                }
                if (Set.of("temporarily_unavailable", "server_error").contains(code)) { return ConnectionFailure.Reason.RETRY_LATER; }
            } catch (java.io.IOException ignored) { /* Never log a provider body. */ }
        }
        return ConnectionFailure.Reason.UNCERTAIN;
    }

    private ConnectionState current(ConnectionKey key) {
        if (closed.get()) { throw failure(ConnectionFailure.Reason.UNAVAILABLE); }
        for (int i = 0; i < 8; i++) {
            var state = store.read(key).orElseGet(() -> new ConnectionState(0, 0, Phase.DISCONNECTED, now(), Optional.empty(), Optional.empty()));
            boolean expiredAttempt = state.phase() == Phase.CONNECTING && state.challenge().orElseThrow().expiresAt() <= now();
            boolean uncertain = state.phase() == Phase.REFRESHING && state.changedAt() + 60_000 <= now();
            if (!expiredAttempt && !uncertain) { return state; }
            var next = uncertain ? state.next(Phase.RENEWAL_UNCERTAIN, now(), state.grant(), Optional.empty())
                : state.next(Phase.REQUIRES_REAUTHORIZATION, now(), Optional.empty(), Optional.empty());
            if (store.append(key, next)) { return next; }
        }
        throw failure(ConnectionFailure.Reason.CONFLICT);
    }

    private String required(String value) {
        return Optional.ofNullable(value).filter(v -> !v.isBlank()).orElseThrow(() -> failure(ConnectionFailure.Reason.REAUTHORIZE));
    }
    private long expires(Long seconds) {
        long lifespan = Optional.ofNullable(seconds).orElseThrow(() -> failure(ConnectionFailure.Reason.REAUTHORIZE));
        if (lifespan <= 0 || lifespan > 86400) { throw failure(ConnectionFailure.Reason.REAUTHORIZE); }
        return now() + lifespan * 1000;
    }
    private Set<String> scopes(String value) {
        return Set.copyOf(Arrays.stream(value.trim().split("\\s+")).filter(v -> !v.isBlank()).toList());
    }
    private void requireScopes(Set<String> scopes) {
        if (!scopes.containsAll(registration.requiredScopes())) { throw failure(ConnectionFailure.Reason.REAUTHORIZE); }
    }
    private void save(ConnectionKey key, ConnectionState state) {
        if (!store.append(key, state)) { throw failure(ConnectionFailure.Reason.CONFLICT); }
    }
    private void invalidate(ConnectionKey key) { synchronized (clients) { clients.remove(key); } }
    private Status status(ConnectionState state) { return new Status(state.phase(), state.revision()); }
    private long now() { return clock.millis(); }
    private ConnectionFailure failure(ConnectionFailure.Reason reason) { return new ConnectionFailure(reason); }
    private Throwable unwrap(Throwable problem) {
        return problem instanceof java.util.concurrent.CompletionException && problem.getCause() != null ? problem.getCause() : problem;
    }
    private <T> CompletionStage<T> offload(Supplier<T> work) { return CompletableFuture.supplyAsync(work, executor); }

    /** Drain calls first. Quarkus owns OidcClient shutdown; the host owns SDK transports and executors. */
    @Override public void close() {
        closed.set(true);
        synchronized (clients) { clients.clear(); }
    }
}
