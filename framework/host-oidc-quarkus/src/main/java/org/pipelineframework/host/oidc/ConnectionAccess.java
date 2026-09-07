package org.pipelineframework.host.oidc;

import io.vertx.ext.web.RoutingContext;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Objects;

/** Application-owned, nonblocking request-thread authorization. External OIDC identity is not the management identity. */
public interface ConnectionAccess {
    enum Action { CONNECT, STATUS, DISCONNECT }
    record Authority(ConnectionKey key, String actor) {
        public Authority {
            Objects.requireNonNull(key);
            Objects.requireNonNull(actor);
            if (actor.isBlank()) { throw new IllegalArgumentException("Actor required"); }
        }
    }
    Authority authorize(SecurityContext security, Action action);
    /** Reauthorize the original application session, not the newly connected external principal. */
    Authority authorizeCompletion(RoutingContext routingContext);
}
