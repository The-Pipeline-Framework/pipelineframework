package org.pipelineframework.host.oidc;

import java.util.Objects;
import org.pipelineframework.connector.ConnectionRef;

/** Host authority key. Never construct its tenant from pipeline inputs or callback parameters. */
public record ConnectionKey(String tenant, ConnectionRef reference) {
    public ConnectionKey {
        Objects.requireNonNull(tenant, "tenant required");
        Objects.requireNonNull(reference, "reference required");
        if (tenant.isBlank() || tenant.length() > 256 || reference.value().length() > 256) {
            throw new IllegalArgumentException("Invalid connection identity");
        }
    }
}
