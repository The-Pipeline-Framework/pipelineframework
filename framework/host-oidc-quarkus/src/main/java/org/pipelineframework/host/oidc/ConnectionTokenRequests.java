package org.pipelineframework.host.oidc;

import io.quarkus.oidc.common.OidcEndpoint;
import io.quarkus.oidc.common.OidcRequestFilter;
import io.smallrye.mutiny.Uni;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Public Quarkus request filter: prevent redispatch after a managed token request's uncertain outcome. */
@OidcEndpoint(OidcEndpoint.Type.TOKEN)
public final class ConnectionTokenRequests implements OidcRequestFilter {
    private final Set<String> configurationNames;
    /** Publish a CDI bean with the managed OIDC tenant/client names. Do not inject clients into this early filter. */
    public ConnectionTokenRequests(Set<String> configurationNames) {
        this.configurationNames = Set.copyOf(configurationNames);
        if (configurationNames.isEmpty() || configurationNames.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("Managed OIDC configuration names required");
        }
    }
    Set<String> configurationNames() { return configurationNames; }
    @Override public Uni<Void> filter(OidcRequestFilterContext context) {
        var properties = context.contextProperties();
        boolean managed = configurationNames.stream().anyMatch(name ->
            name.equals(properties.getString("tenant-id")) || name.equals(properties.getString("client-id")));
        if (!managed) { return Uni.createFrom().voidItem(); }
        var sent = new AtomicBoolean();
        // 3.33.1 rejects retry-count=0. Its retry resubscribes to this Uni before sendBuffer.
        return Uni.createFrom().deferred(() -> sent.compareAndSet(false, true) ? Uni.createFrom().voidItem()
            : Uni.createFrom().failure(new ConnectionFailure(ConnectionFailure.Reason.UNCERTAIN)));
    }
}
