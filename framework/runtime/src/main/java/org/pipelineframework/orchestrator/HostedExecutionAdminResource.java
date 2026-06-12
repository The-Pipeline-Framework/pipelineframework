package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pipelineframework.orchestrator.dto.HostedExecutionRedriveRequest;

/**
 * Default-disabled admin API for operator-controlled execution operations.
 */
@ApplicationScoped
@Path("/tpf/admin/tenants/{tenantId}/executions")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class HostedExecutionAdminResource {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineControlPlane controlPlane;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    @PostConstruct
    void validateConfig() {
        if (!enabled()) {
            return;
        }
        WorkerSecretSupport.validationError(
            orchestratorConfig.admin().adminToken(),
            orchestratorConfig.admin().adminTokenRef(),
            "pipeline.orchestrator.admin.admin-token",
            "pipeline.orchestrator.admin.admin-token-ref",
            "pipeline.orchestrator.admin.enabled=true")
            .ifPresent(message -> {
                throw new IllegalStateException(message);
            });
    }

    @POST
    @Path("/{executionId}/redrive")
    @Blocking
    public Uni<Response> redrive(
        @PathParam("tenantId") String tenantId,
        @PathParam("executionId") String executionId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization,
        HostedExecutionRedriveRequest request) {
        Optional<Response> guard = guard(tenantId, executionId, authorization);
        if (guard.isPresent()) {
            return Uni.createFrom().item(guard.get());
        }
        HostedExecutionRedriveRequest effectiveRequest = request == null
            ? new HostedExecutionRedriveRequest(null, null, false)
            : request;
        return controlPlane.redriveExecution(
                tenantId,
                executionId,
                effectiveRequest.expectedVersion(),
                effectiveRequest.allowFailed(),
                effectiveRequest.reason())
            .onItem().transform(result -> Response.ok(result).build())
            .onFailure(NotFoundException.class).recoverWithItem(failure ->
                Response.status(Response.Status.NOT_FOUND).entity(failure.getMessage()).build())
            .onFailure(IllegalStateException.class).recoverWithItem(failure ->
                Response.status(Response.Status.CONFLICT).entity(failure.getMessage()).build())
            .onFailure(IllegalArgumentException.class).recoverWithItem(failure ->
                Response.status(Response.Status.BAD_REQUEST).entity(failure.getMessage()).build());
    }

    private Optional<Response> guard(String tenantId, String executionId, String authorization) {
        if (!enabled()) {
            return Optional.of(Response.status(Response.Status.NOT_FOUND).build());
        }
        if (tenantId == null || tenantId.isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("tenantId is required").build());
        }
        if (executionId == null || executionId.isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("executionId is required").build());
        }
        return authenticate(authorization);
    }

    private Optional<Response> authenticate(String authorization) {
        if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
            return Optional.of(Response.status(Response.Status.UNAUTHORIZED).build());
        }
        String presented = authorization.substring(BEARER_PREFIX.length()).trim();
        if (presented.isBlank()) {
            return Optional.of(Response.status(Response.Status.UNAUTHORIZED).build());
        }
        String expected;
        try {
            expected = WorkerSecretSupport.resolve(
                orchestratorConfig.admin().adminToken(),
                orchestratorConfig.admin().adminTokenRef(),
                secretResolver,
                "pipeline.orchestrator.admin.admin-token",
                "pipeline.orchestrator.admin.admin-token-ref");
        } catch (RuntimeException e) {
            return Optional.of(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("Execution admin token is unavailable").build());
        }
        if (!MessageDigest.isEqual(
            expected.getBytes(StandardCharsets.UTF_8),
            presented.getBytes(StandardCharsets.UTF_8))) {
            return Optional.of(Response.status(Response.Status.UNAUTHORIZED).build());
        }
        return Optional.empty();
    }

    private boolean enabled() {
        return orchestratorConfig != null
            && orchestratorConfig.admin() != null
            && orchestratorConfig.admin().enabled();
    }
}
