package org.pipelineframework.host.oidc;

import io.quarkus.oidc.AuthenticationCompletionAction;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;

/** Fail early rather than allowing Quarkus 3.33.1 to silently omit ambiguous completion actions. */
@ApplicationScoped
public class ConnectionHookValidation {
    void validate(@Observes StartupEvent started, Instance<ConnectionOidcHooks> hooks,
                  Instance<AuthenticationCompletionAction> actions, Instance<ConnectionTokenRequests> tokenRequests) {
        if (!hooks.isUnsatisfied() && (hooks.isAmbiguous() || actions.isAmbiguous())) {
            throw new IllegalStateException("Publish one ConnectionOidcHooks bean containing all managed registrations; "
                + "Quarkus 3.33.1 requires a single AuthenticationCompletionAction bean");
        }
        if (hooks.isResolvable() && (!tokenRequests.isResolvable()
            || !tokenRequests.get().configurationNames().containsAll(hooks.get().configurationNames()))) {
            throw new IllegalStateException("Publish ConnectionTokenRequests for every managed OIDC configuration name");
        }
    }
}
