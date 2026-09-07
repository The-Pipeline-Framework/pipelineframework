package org.pipelineframework.host.oidc;

import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.pipelineframework.connector.ResolvedConnection;

/** Host mapping to existing Quarkus configuration, not an OAuth provider profile. */
public record ConnectionRegistration<C extends ResolvedConnection>(String tenantId, String clientId, URI issuer,
        URI flowUri, Set<String> requiredScopes, Class<C> connectionType,
        Predicate<JsonWebToken> accountPolicy, ClientFactory<C> factory) {
    public ConnectionRegistration {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(clientId);
        Objects.requireNonNull(issuer);
        Objects.requireNonNull(flowUri);
        requiredScopes = Set.copyOf(requiredScopes);
        Objects.requireNonNull(connectionType);
        Objects.requireNonNull(accountPolicy);
        Objects.requireNonNull(factory);
        if (!tenantId.matches("[a-zA-Z0-9_-]+") || clientId.isBlank() || !issuer.isAbsolute()
            || issuer.getHost() == null || issuer.getUserInfo() != null
            || !Set.of("http", "https").contains(issuer.getScheme())
            || flowUri.getUserInfo() != null || flowUri.getPath() == null || flowUri.getPath().isBlank()
            || flowUri.getScheme() == null || !Set.of("http", "https").contains(flowUri.getScheme())
            || flowUri.getHost() == null || flowUri.getRawQuery() != null
            || flowUri.getRawFragment() != null || requiredScopes.isEmpty()) {
            throw new IllegalArgumentException("Invalid host connection registration");
        }
    }
    /** Stable shared-store authority, even if Quarkus configuration aliases change. */
    public String storageId() { return issuer + "|" + clientId; }

    @FunctionalInterface
    public interface ClientFactory<C extends ResolvedConnection> {
        /** Construct without I/O. The client must invoke accessToken before EVERY outgoing request; never refresh itself. */
        C create(RequestAccess access);
    }

    /** Host-only access for client construction. Never returned to a Connector. */
    public interface RequestAccess {
        /** Blocking: called only on the host/Connector blocking executor. Checks durable authority. */
        String accessToken();
        /** Report a trusted resource's authentication challenge without exposing its raw body to telemetry. */
        void requiresInteraction();
    }
}
