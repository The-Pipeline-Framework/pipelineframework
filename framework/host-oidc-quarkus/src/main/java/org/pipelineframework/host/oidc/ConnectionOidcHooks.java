package org.pipelineframework.host.oidc;

import io.quarkus.oidc.AuthenticationCompletionAction;
import io.quarkus.oidc.OidcRedirectFilter;
import io.smallrye.mutiny.Uni;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.eclipse.microprofile.jwt.JsonWebToken;

/** Publish ONE CDI bean for all registrations. Quarkus 3.33.1 skips ambiguous completion-action beans. */
public final class ConnectionOidcHooks implements OidcRedirectFilter, AuthenticationCompletionAction {
    public record Binding(QuarkusConnections connections, ConnectionAccess access) { }
    private final java.util.List<ConnectionOidcHooks> delegates;
    private final QuarkusConnections connections;
    private final ConnectionAccess access;
    private final ConnectionBrowser browser;
    public ConnectionOidcHooks(QuarkusConnections connections, ConnectionAccess access) {
        delegates = java.util.List.of();
        this.connections = Objects.requireNonNull(connections);
        this.access = Objects.requireNonNull(access);
        browser = new ConnectionBrowser(connections.registration());
    }
    public ConnectionOidcHooks(java.util.List<Binding> bindings) {
        if (bindings.isEmpty() || bindings.stream().map(binding -> binding.connections().registration().flowUri().getPath())
            .distinct().count() != bindings.size()
            || bindings.stream().map(binding -> binding.connections().registration().tenantId()).distinct().count() != bindings.size()) { throw new IllegalArgumentException("Unique managed flow paths and OIDC tenants required"); }
        connections = Objects.requireNonNull(bindings.getFirst().connections());
        access = Objects.requireNonNull(bindings.getFirst().access());
        browser = new ConnectionBrowser(connections.registration());
        delegates = bindings.stream().map(binding -> new ConnectionOidcHooks(binding.connections(), binding.access())).toList();
    }
    java.util.Set<String> configurationNames() {
        return delegates.isEmpty() ? java.util.Set.of(connections.registration().tenantId())
            : delegates.stream().map(hook -> hook.connections.registration().tenantId()).collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
    @Override public void filter(OidcRedirectContext context) {
        if (!delegates.isEmpty()) { delegates.forEach(hook -> hook.filter(context)); return; }
        if (!connections.registration().flowUri().getPath().equals(context.routingContext().request().path())) { return; }
        var config = context.oidcTenantConfig();
        var registration = connections.registration();
        if (!config.tenantId().orElse("").equals(registration.tenantId())
            || !config.clientId().orElse("").equals(registration.clientId())
            || !config.tokenStateManager().strategy().name().equals("ID_TOKEN")
            || config.token().refreshExpired() || config.token().refreshTokenTimeSkew().isPresent()
            || !config.authentication().pkceRequired().orElse(false)) {
            throw new IllegalStateException("Managed connection flow requires matching OIDC tenant/client, PKCE, ID-only session and no session refresh");
        }
        // Public redirect context contains the URL Quarkus constructed, including its generated state.
        String query = Objects.requireNonNullElse(URI.create(context.redirectUri()).getRawQuery(), "");
        var state = Arrays.stream(query.split("&")).map(pair -> pair.split("=", 2))
            .filter(pair -> pair.length == 2 && pair[0].equals("state"))
            .map(pair -> URLDecoder.decode(pair[1], StandardCharsets.UTF_8)).findFirst();
        // Ignore local post-authentication/logout redirects, which have no authorization state.
        state.ifPresent(value -> browser.write(context.routingContext(), connections.bind(browser.read(context.routingContext()), value)));
    }
    @Override public Uni<Void> action(AuthenticationCompletionContext context) {
        if (!delegates.isEmpty()) {
            Uni<Void> result = Uni.createFrom().voidItem();
            for (var hook : delegates) { result = result.chain(() -> hook.action(context)); }
            return result;
        }
        if (!connections.registration().flowUri().getPath().equals(context.routingContext().request().path())) {
            return Uni.createFrom().voidItem();
        }
        try {
            var attempt = connections.decode(browser.read(context.routingContext()));
            var authority = access.authorizeCompletion(context.routingContext());
            if (!authority.key().equals(attempt.key()) || !authority.actor().equals(attempt.actor())
                || !(context.identity().getPrincipal() instanceof JsonWebToken identity)) {
                throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
            }
            String state = context.routingContext().request().getParam("state");
            return Uni.createFrom().completionStage(connections.complete(attempt, state, identity, context.tokens()))
                .onFailure().transform(problem -> problem instanceof ConnectionFailure ? problem
                    : new ConnectionFailure(ConnectionFailure.Reason.INVALID_CALLBACK)).replaceWithVoid();
        } catch (RuntimeException problem) {
            java.util.logging.Logger.getLogger(ConnectionOidcHooks.class.getName()).log(java.util.logging.Level.WARNING,
                "Connection completion rejected ({0})", problem instanceof ConnectionFailure failure ? failure.reason() : problem.getClass().getName());
            // Do not pass untrusted host errors or token responses into Quarkus authentication logs.
            return Uni.createFrom().failure(new ConnectionFailure(ConnectionFailure.Reason.INVALID_CALLBACK));
        }
    }
}
