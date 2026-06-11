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
import org.pipelineframework.orchestrator.dto.HostedBundleRegisterRequest;

/**
 * Default-disabled local/dev admin API for registering and activating hosted bundle artifacts.
 */
@ApplicationScoped
@Path("/tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/bundles")
@Produces(MediaType.APPLICATION_JSON)
public class HostedBundleAdminResource {

    private static final Logger LOG = Logger.getLogger(HostedBundleAdminResource.class);
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineBundleRegistry bundleRegistry;

    @Inject
    PipelineBundleRegistrar bundleRegistrar;

    @Inject
    PipelineBundleArtifactStore artifactStore;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    private volatile PipelineBundleRegistry fallbackRegistry;
    private volatile PipelineBundleRegistrar fallbackRegistrar;
    private volatile PipelineBundleArtifactStore fallbackArtifactStore;

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
        HostedBundleRegisterRequest request) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        if (request == null || request.artifactPath() == null || request.artifactPath().isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("artifactPath is required").build());
        }
        return registrar().validateAsync(tenantId, pipelineId, request.artifactPath(), System.currentTimeMillis())
            .onFailure(IllegalArgumentException.class).recoverWithUni(failure -> invalidRegistration(failure))
            .onFailure(IllegalStateException.class).recoverWithUni(failure -> invalidRegistration(failure))
            .onItem().transformToUni(record -> registry().register(record))
            .onItem().transform(registered -> Response.ok(registered).build())
            .onFailure(InvalidBundleRegistrationException.class).recoverWithItem(failure ->
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
    @Path("/{bundleVersionId}")
    @Blocking
    public Uni<Response> get(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @PathParam("bundleVersionId") String bundleVersionId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return registry().get(tenantId, pipelineId, bundleVersionId)
            .onItem().transform(record -> record
                .map(value -> Response.ok(value).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build()));
    }

    @POST
    @Path("/{bundleVersionId}/activate")
    @Blocking
    public Uni<Response> activate(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @PathParam("bundleVersionId") String bundleVersionId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Response guard = guard(tenantId, pipelineId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return registry().get(tenantId, pipelineId, bundleVersionId)
            .onItem().transformToUni(record -> {
                if (record.isEmpty()) {
                    return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
                }
                try {
                    artifactStore().verify(record.get());
                } catch (IllegalArgumentException | IllegalStateException e) {
                    LOG.warnf("Invalid hosted bundle activation request: %s", e.getMessage());
                    return Uni.createFrom().item(Response.status(Response.Status.CONFLICT)
                        .entity(e.getMessage()).build());
                }
                return registry().activate(tenantId, pipelineId, bundleVersionId, System.currentTimeMillis())
                    .onItem().transform(active -> active
                        .map(value -> Response.ok(value).build())
                        .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build()));
            });
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
                .entity("Hosted bundle admin token is unavailable").build();
        }
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
        byte[] presentedBytes = presented.getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expectedBytes, presentedBytes)) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        return null;
    }

    private boolean enabled() {
        return orchestratorConfig != null
            && orchestratorConfig.admin() != null
            && orchestratorConfig.admin().enabled();
    }

    private PipelineBundleRegistry registry() {
        if (bundleRegistry != null) {
            return bundleRegistry;
        }
        PipelineBundleRegistry fallback = fallbackRegistry;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackRegistry;
                if (fallback == null) {
                    fallback = new InMemoryPipelineBundleRegistry();
                    fallbackRegistry = fallback;
                }
            }
        }
        return fallback;
    }

    private PipelineBundleRegistrar registrar() {
        if (bundleRegistrar != null) {
            return bundleRegistrar;
        }
        PipelineBundleRegistrar fallback = fallbackRegistrar;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackRegistrar;
                if (fallback == null) {
                    fallback = new PipelineBundleRegistrar();
                    fallbackRegistrar = fallback;
                }
            }
        }
        return fallback;
    }

    private PipelineBundleArtifactStore artifactStore() {
        if (artifactStore != null) {
            return artifactStore;
        }
        PipelineBundleArtifactStore fallback = fallbackArtifactStore;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackArtifactStore;
                if (fallback == null) {
                    fallback = new LocalPipelineBundleArtifactStore(
                        PipelineBundleRuntimeBeans.storageRoot(orchestratorConfig),
                        new PipelineBundleManifestLoader());
                    fallbackArtifactStore = fallback;
                }
            }
        }
        return fallback;
    }

    private Uni<PipelineBundleRecord> invalidRegistration(Throwable failure) {
        LOG.warnf("Invalid hosted bundle registration request: %s", failure.getMessage());
        return Uni.createFrom().failure(new InvalidBundleRegistrationException(failure.getMessage()));
    }

    private static final class InvalidBundleRegistrationException extends RuntimeException {
        private InvalidBundleRegistrationException(String message) {
            super(message);
        }
    }
}
