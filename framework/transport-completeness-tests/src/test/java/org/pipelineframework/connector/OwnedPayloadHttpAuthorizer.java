package org.pipelineframework.connector;

import jakarta.enterprise.context.ApplicationScoped;

/** Test host policy: alice owns one tenant and one business scope. */
@ApplicationScoped
public final class OwnedPayloadHttpAuthorizer implements PayloadBoundaryAuthorizer {
    @Override
    public PayloadBoundaryOwner authorize(PayloadBoundaryAuthorizationRequest request) {
        if (!"alice".equals(request.principalName())
            || !"tenant-a".equals(request.requestedTenantId())
            || !"invoice-1".equals(request.requestedScopeId())) {
            throw new SecurityException("test principal cannot access the requested owner");
        }
        return new PayloadBoundaryOwner("tenant-a", "invoice-1");
    }
}
