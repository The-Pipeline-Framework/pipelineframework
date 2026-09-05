package org.pipelineframework.host.gmail;

import java.net.URI;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.gmail.AuthenticatedGmailConnection;

/** Optional host lifecycle service. All storage and OAuth work is offloaded to a host blocking executor. */
public final class GmailConnections implements AutoCloseable {
    public enum Phase { DISCONNECTED, CONNECTING, EXCHANGING, READY, REFRESHING, REQUIRES_REAUTHORIZATION }
    public record Status(Phase phase, long revision) { }
    public record AuthorizationStart(URI redirect, String browserSecret) {
        @Override
        public String toString() { return "AuthorizationStart[redacted]"; }
    }

    private record Cached(long revision, AuthenticatedGmailConnection connection) { }
    private final JdbcGmailConnectionStore store;
    private final GmailAuthorization authorization;
    private final Executor blockingExecutor;
    private final Clock clock;
    private final Map<ConnectionKey, Cached> clients = new LinkedHashMap<>(16, 0.75f, true);
    private final AtomicBoolean closed = new AtomicBoolean();

    public GmailConnections(JdbcGmailConnectionStore store, GmailAuthorization authorization,
                            Executor blockingExecutor, Clock clock) {
        this.store = Objects.requireNonNull(store);
        this.authorization = Objects.requireNonNull(authorization);
        this.blockingExecutor = Objects.requireNonNull(blockingExecutor);
        this.clock = Objects.requireNonNull(clock);
        if (!store.registrationId().equals(authorization.registrationId())) {
            throw new IllegalArgumentException("Store and Google OAuth registration must match");
        }
    }

    /** Call only after the host authorizes this actor to manage this tenant's logical connection. */
    public CompletionStage<AuthorizationStart> begin(ConnectionKey key, String actor) {
        return offload(() -> {
            if (actor == null || actor.isBlank()) { throw failure(ConnectionFailure.Reason.FORBIDDEN); }
            ConnectionState previous = current(key);
            String state = store.encryption().nonce();
            String browser = store.encryption().nonce();
            String verifier = store.encryption().nonce();
            URI redirect = authorization.authorize(state, verifier);
            var challenge = new ConnectionState.Challenge(store.encryption().hash(state),
                store.encryption().hash(browser), actor, verifier, now() + 600_000);
            save(key, previous.next(Phase.CONNECTING, now(), previous.grant(), Optional.of(challenge)));
            return new AuthorizationStart(redirect, browser);
        });
    }

    /** The key and actor must come from the authenticated host session, never from Google's callback. */
    public CompletionStage<Status> complete(ConnectionKey key, String actor, String state,
                                             String browserSecret, String code) {
        return offload(() -> {
            ConnectionState pending = current(key);
            var challenge = pending.challenge().orElseThrow(() -> failure(ConnectionFailure.Reason.INVALID_CALLBACK));
            if (pending.phase() != Phase.CONNECTING || challenge.expiresAt() <= now()
                || !challenge.actor().equals(actor) || code == null || code.isBlank()
                || state == null || browserSecret == null
                || !challenge.stateHash().equals(store.encryption().hash(state))
                || !challenge.browserHash().equals(store.encryption().hash(browserSecret))) {
                throw failure(ConnectionFailure.Reason.INVALID_CALLBACK);
            }
            ConnectionState claimed = pending.next(Phase.EXCHANGING, now(), pending.grant(), Optional.empty());
            save(key, claimed);
            try {
                var grant = authorization.exchange(code, challenge.verifier(), pending.grant());
                store.claimAccount(key, grant.subject());
                ConnectionState ready = claimed.next(Phase.READY, now(), Optional.of(grant), Optional.empty());
                save(key, ready);
                return status(ready);
            } catch (RuntimeException failure) {
                // A consumed code cannot be retried. A concurrent disconnect/reconnect must win.
                store.append(key, claimed.next(Phase.REQUIRES_REAUTHORIZATION, now(), Optional.empty(), Optional.empty()));
                throw safe(failure);
            }
        });
    }

    public CompletionStage<Status> status(ConnectionKey key) {
        return offload(() -> status(current(key)));
    }

    /** Local disconnect only. Google grant revocation is broader and remains an explicit host action. */
    public CompletionStage<Status> disconnect(ConnectionKey key) {
        return offload(() -> {
            for (int attempt = 0; attempt < 8; attempt++) {
                ConnectionState previous = current(key);
                ConnectionState disconnected = previous.next(Phase.DISCONNECTED, now(), Optional.empty(), Optional.empty());
                if (store.append(key, disconnected)) {
                    invalidate(key);
                    return status(disconnected);
                }
            }
            throw failure(ConnectionFailure.Reason.CONFLICT);
        });
    }

    /** Invoke after host policy validates the invocation target. This helper installs no CDI resolver. */
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        CompletionStage<C> stage = offload(() -> {
            if (request.connectionType() != AuthenticatedGmailConnection.class) {
                throw failure(ConnectionFailure.Reason.FORBIDDEN);
            }
            String tenant = request.invocationContext().tenantId()
                .orElseThrow(() -> failure(ConnectionFailure.Reason.FORBIDDEN));
            ConnectionKey key = new ConnectionKey(tenant, request.reference());
            ConnectionState ready = usable(key);
            synchronized (clients) {
                Cached cached = clients.get(key);
                if (cached == null || cached.revision() != ready.revision()) {
                    var client = authorization.client(ready.grant().orElseThrow(), () -> {
                        // This read deliberately has no TTL: another host may have disconnected the grant.
                        // Gmail performs blocking SDK calls on the Connector's blocking executor.
                        ConnectionState actual = current(key);
                        if (closed.get() || actual.phase() != Phase.READY || actual.revision() != ready.revision()
                            || actual.grant().orElseThrow().expiresAt() <= now()) {
                            throw failure(ConnectionFailure.Reason.UNAVAILABLE);
                        }
                    });
                    cached = new Cached(ready.revision(), new AuthenticatedGmailConnection(client));
                    clients.put(key, cached);
                    if (clients.size() > 128) { clients.remove(clients.keySet().iterator().next()); }
                }
                return request.connectionType().cast(cached.connection());
            }
        });
        return stage.exceptionallyCompose(problem -> {
            Throwable cause = problem instanceof java.util.concurrent.CompletionException ? problem.getCause() : problem;
            ConnectionFailure.Reason reason = cause instanceof ConnectionFailure failure
                ? failure.reason() : ConnectionFailure.Reason.UNAVAILABLE;
            var kind = switch (reason) {
                case REAUTHORIZE -> ConnectionResolutionException.Kind.AUTHENTICATION_REQUIRED;
                case FORBIDDEN, INVALID_CALLBACK -> ConnectionResolutionException.Kind.CONFIGURATION;
                default -> ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE;
            };
            return CompletableFuture.failedStage(new ConnectionResolutionException(kind, "gmail-connection-" + reason.name()));
        });
    }

    private ConnectionState usable(ConnectionKey key) {
        for (int attempt = 0; attempt < 100; attempt++) {
            ConnectionState state = current(key);
            if (state.phase() == Phase.REFRESHING) {
                try { Thread.sleep(20); } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw failure(ConnectionFailure.Reason.UNAVAILABLE);
                }
                continue;
            }
            if (state.phase() != Phase.READY) { throw failure(ConnectionFailure.Reason.REAUTHORIZE); }
            var grant = state.grant().orElseThrow(() -> failure(ConnectionFailure.Reason.STORAGE));
            if (grant.expiresAt() > now() + 60_000) { return state; }
            ConnectionState claimed = state.next(Phase.REFRESHING, now(), state.grant(), Optional.empty());
            if (!store.append(key, claimed)) { continue; }
            try {
                var renewed = authorization.refresh(grant);
                if (!renewed.subject().equals(grant.subject())) { throw failure(ConnectionFailure.Reason.REAUTHORIZE); }
                ConnectionState ready = claimed.next(Phase.READY, now(), Optional.of(renewed), Optional.empty());
                save(key, ready);
                return ready;
            } catch (ConnectionFailure failure) {
                if (failure.reason() == ConnectionFailure.Reason.REAUTHORIZE) {
                    store.append(key, claimed.next(Phase.REQUIRES_REAUTHORIZATION, now(), Optional.empty(), Optional.empty()));
                } else if (failure.reason() == ConnectionFailure.Reason.RETRY_LATER) {
                    store.append(key, claimed.next(Phase.READY, now(), state.grant(), Optional.empty()));
                }
                // Unknown remote outcome stays REFRESHING. Do not retry a potentially consumed grant.
                throw failure;
            } catch (RuntimeException failure) {
                throw failure(ConnectionFailure.Reason.UNAVAILABLE);
            }
        }
        throw failure(ConnectionFailure.Reason.UNAVAILABLE);
    }

    private ConnectionState current(ConnectionKey key) {
        if (closed.get()) { throw failure(ConnectionFailure.Reason.UNAVAILABLE); }
        for (int attempt = 0; attempt < 8; attempt++) {
            ConnectionState state = store.read(key).orElseGet(() ->
                new ConnectionState(0, Phase.DISCONNECTED, now(), Optional.empty(), Optional.empty()));
            boolean expired = switch (state.phase()) {
                case CONNECTING -> state.challenge().orElseThrow().expiresAt() <= now();
                case EXCHANGING, REFRESHING -> state.changedAt() + 60_000 <= now();
                default -> false;
            };
            if (!expired) { return state; }
            ConnectionState unusable = state.phase() == Phase.CONNECTING && state.grant().isPresent()
                ? state.next(Phase.READY, now(), state.grant(), Optional.empty())
                : state.next(Phase.REQUIRES_REAUTHORIZATION, now(), Optional.empty(), Optional.empty());
            if (store.append(key, unusable)) { invalidate(key); return unusable; }
        }
        throw failure(ConnectionFailure.Reason.UNAVAILABLE);
    }

    private void save(ConnectionKey key, ConnectionState state) {
        if (!store.append(key, state)) { throw failure(ConnectionFailure.Reason.CONFLICT); }
        invalidate(key);
    }

    private void invalidate(ConnectionKey key) { synchronized (clients) { clients.remove(key); } }
    private Status status(ConnectionState state) { return new Status(state.phase(), state.revision()); }
    private long now() { return clock.millis(); }
    private ConnectionFailure failure(ConnectionFailure.Reason reason) { return new ConnectionFailure(reason); }
    private ConnectionFailure safe(RuntimeException failure) {
        return failure instanceof ConnectionFailure safe ? safe : failure(ConnectionFailure.Reason.UNAVAILABLE);
    }
    private <T> CompletionStage<T> offload(Supplier<T> operation) {
        if (closed.get()) { return CompletableFuture.failedStage(failure(ConnectionFailure.Reason.UNAVAILABLE)); }
        return CompletableFuture.supplyAsync(operation, blockingExecutor);
    }

    /** Host shutdown only, after draining invocations. Gmail clients share the host transport. */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            synchronized (clients) { clients.clear(); }
            authorization.close();
        }
    }
}
