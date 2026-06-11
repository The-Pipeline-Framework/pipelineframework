package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.dto.HostedReleaseRegisterRequest;

/**
 * Default-disabled local/dev admin API for registering and activating pipeline releases.
 */
@ApplicationScoped
@Path("/tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases")
@Produces(MediaType.APPLICATION_JSON)
public class HostedReleaseAdminResource {

    private static final Logger LOG = Logger.getLogger(HostedReleaseAdminResource.class);
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineReleaseRegistry releaseRegistry;

    @Inject
    PipelineReleaseRegistrar releaseRegistrar;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    private volatile PipelineReleaseRegistry fallbackRegistry;
    private volatile PipelineReleaseRegistrar fallbackRegistrar;

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
    @Path("/register")
    @Consumes(MediaType.APPLICATION_JSON)
    @Blocking
    public Uni<Response> register(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization,
        HostedReleaseRegisterRequest request) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        if (request == null || request.releaseDescriptorPath() == null || request.releaseDescriptorPath().isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("releaseDescriptorPath is required").build());
        }
        return registrar().validateAsync(tenantId, pipelineId, request.releaseDescriptorPath(), System.currentTimeMillis())
            .onFailure(IllegalArgumentException.class).recoverWithUni(failure -> invalidRegistration(failure))
            .onFailure(IllegalStateException.class).recoverWithUni(failure -> invalidRegistration(failure))
            .onItem().transformToUni(record -> registry().register(record))
            .onItem().transform(registered -> Response.ok(registered).build())
            .onFailure(InvalidReleaseRegistrationException.class).recoverWithItem(failure ->
                Response.status(Response.Status.BAD_REQUEST).entity(failure.getMessage()).build())
            .onFailure(IllegalStateException.class).recoverWithItem(failure ->
                Response.status(Response.Status.CONFLICT).entity(failure.getMessage()).build());
    }

    @GET
    @Blocking
    public Uni<Response> list(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return registry().list(tenantId, pipelineId)
            .onItem().transform(records -> Response.ok(records).build());
    }

    @GET
    @Path("/active")
    @Blocking
    public Uni<Response> active(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return registry().active(tenantId, pipelineId)
            .onItem().transform(record -> record
                .map(value -> Response.ok(value).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build()));
    }

    @GET
    @Path("/{releaseVersion}")
    @Blocking
    public Uni<Response> get(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @PathParam("releaseVersion") String releaseVersion,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return registry().get(tenantId, pipelineId, releaseVersion)
            .onItem().transform(record -> record
                .map(value -> Response.ok(value).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build()));
    }

    @POST
    @Path("/{releaseVersion}/activate")
    @Blocking
    public Uni<Response> activate(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @PathParam("releaseVersion") String releaseVersion,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return registry().get(tenantId, pipelineId, releaseVersion)
            .onItem().transformToUni(record -> {
                if (record.isEmpty()) {
                    return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
                }
                try {
                    registrar().verify(record.get());
                } catch (IllegalArgumentException | IllegalStateException e) {
                    LOG.warnf("Invalid release activation request: %s", e.getMessage());
                    return Uni.createFrom().item(Response.status(Response.Status.CONFLICT)
                        .entity(e.getMessage()).build());
                }
                return registry().activate(tenantId, pipelineId, releaseVersion, System.currentTimeMillis())
                    .onItem().transform(active -> active
                        .map(value -> Response.ok(value).build())
                        .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build()));
            });
    }

    private Uni<PipelineReleaseRecord> invalidRegistration(Throwable failure) {
        return Uni.createFrom().failure(new InvalidReleaseRegistrationException(failure.getMessage(), failure));
    }

    private Response guard(String tenantId, String pipelineId, String authorization) {
        if (!enabled()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        if (tenantId == null || tenantId.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("tenantId is required").build();
        }
        if (pipelineId == null || pipelineId.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("pipelineId is required").build();
        }
        return authenticate(authorization);
    }

    private Response authenticate(String authorization) {
        if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        String presented = authorization.substring(BEARER_PREFIX.length()).trim();
        if (presented.isBlank()) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
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
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("Release admin token is unavailable").build();
        }
        if (!MessageDigest.isEqual(
            expected.getBytes(StandardCharsets.UTF_8),
            presented.getBytes(StandardCharsets.UTF_8))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        return null;
    }

    private boolean enabled() {
        return orchestratorConfig != null
            && orchestratorConfig.admin() != null
            && orchestratorConfig.admin().enabled();
    }

    private PipelineReleaseRegistry registry() {
        if (releaseRegistry != null) {
            return releaseRegistry;
        }
        PipelineReleaseRegistry fallback = fallbackRegistry;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackRegistry;
                if (fallback == null) {
                    fallback = new InMemoryPipelineReleaseRegistry();
                    fallbackRegistry = fallback;
                }
            }
        }
        return fallback;
    }

    private PipelineReleaseRegistrar registrar() {
        if (releaseRegistrar != null) {
            return releaseRegistrar;
        }
        PipelineReleaseRegistrar fallback = fallbackRegistrar;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackRegistrar;
                if (fallback == null) {
                    fallback = new PipelineReleaseRegistrar();
                    fallbackRegistrar = fallback;
                }
            }
        }
        return fallback;
    }

    private static final class InvalidReleaseRegistrationException extends RuntimeException {

        private InvalidReleaseRegistrationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
