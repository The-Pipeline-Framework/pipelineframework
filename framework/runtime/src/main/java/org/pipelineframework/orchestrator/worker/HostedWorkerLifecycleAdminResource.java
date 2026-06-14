package org.pipelineframework.orchestrator.worker;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;

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
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pipelineframework.orchestrator.ControlPlaneSecretResolver;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseRuntimeBeans;
import org.pipelineframework.orchestrator.WorkerSecretSupport;
import org.pipelineframework.orchestrator.worker.dto.HostedWorkerRegisterRequest;

/**
 * Default-disabled local/dev admin API for worker lifecycle records.
 */
@ApplicationScoped
@Path("/tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/workers")
@Produces(MediaType.APPLICATION_JSON)
public class HostedWorkerLifecycleAdminResource {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineWorkerRegistry workerRegistry;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    private volatile PipelineWorkerRegistry fallbackRegistry;

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
        HostedWorkerRegisterRequest request) {
        Optional<Response> guard = guard(tenantId, pipelineId, authorization);
        if (guard.isPresent()) {
            return Uni.createFrom().item(guard.get());
        }
        Optional<Response> requestError = validateRegistrationRequest(request);
        if (requestError.isPresent()) {
            return Uni.createFrom().item(requestError.get());
        }
        PipelineWorkerRegistration registration = new PipelineWorkerRegistration(
            tenantId,
            pipelineId,
            request.contractVersion(),
            request.releaseVersion(),
            request.workerId(),
            request.protocol(),
            request.endpoint(),
            request.artifactId(),
            request.artifactDigest());
        return registry().register(registration, System.currentTimeMillis())
            .onItem().transform(record -> Response.ok(record).build())
            .onFailure(IllegalArgumentException.class).recoverWithItem(failure ->
                Response.status(Response.Status.BAD_REQUEST).entity(failure.getMessage()).build());
    }

    @POST
    @Path("/{workerId}/heartbeat")
    @Blocking
    public Uni<Response> heartbeat(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @PathParam("workerId") String workerId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Optional<Response> guard = guard(tenantId, pipelineId, authorization);
        if (guard.isPresent()) {
            return Uni.createFrom().item(guard.get());
        }
        if (workerId == null || workerId.isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("workerId is required").build());
        }
        return registry().heartbeat(
                tenantId,
                pipelineId,
                workerId,
                System.currentTimeMillis(),
                PipelineReleaseRuntimeBeans.workerStaleAfter(orchestratorConfig))
            .onItem().transform(record -> record
                .map(value -> Response.ok(value).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build()));
    }

    @POST
    @Path("/{workerId}/drain")
    @Blocking
    public Uni<Response> markDraining(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @PathParam("workerId") String workerId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization) {
        Optional<Response> guard = guard(tenantId, pipelineId, authorization);
        if (guard.isPresent()) {
            return Uni.createFrom().item(guard.get());
        }
        if (workerId == null || workerId.isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("workerId is required").build());
        }
        return registry().markDraining(
                tenantId,
                pipelineId,
                workerId,
                System.currentTimeMillis(),
                PipelineReleaseRuntimeBeans.workerStaleAfter(orchestratorConfig))
            .onItem().transform(record -> record
                .map(value -> Response.ok(value).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build()));
    }

    @GET
    @Blocking
    public Uni<Response> list(
        @PathParam("tenantId") String tenantId,
        @PathParam("pipelineId") String pipelineId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization,
        @QueryParam("contractVersion") String contractVersion,
        @QueryParam("releaseVersion") String releaseVersion) {
        Optional<Response> guard = guard(tenantId, pipelineId, authorization);
        if (guard.isPresent()) {
            return Uni.createFrom().item(guard.get());
        }
        long now = System.currentTimeMillis();
        return registry().list(tenantId, pipelineId, now, PipelineReleaseRuntimeBeans.workerStaleAfter(orchestratorConfig))
            .onItem().transform(records -> Response.ok(records.stream()
                .filter(record -> contractVersion == null
                    || contractVersion.isBlank()
                    || record.contractVersion().equals(contractVersion))
                .filter(record -> releaseVersion == null
                    || releaseVersion.isBlank()
                    || record.releaseVersion().equals(releaseVersion))
                .toList()).build());
    }

    private Optional<Response> validateRegistrationRequest(HostedWorkerRegisterRequest request) {
        if (request == null) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("request is required").build());
        }
        if (request.workerId() == null || request.workerId().isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("workerId is required").build());
        }
        if (request.contractVersion() == null || request.contractVersion().isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("contractVersion is required").build());
        }
        if (request.releaseVersion() == null || request.releaseVersion().isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("releaseVersion is required").build());
        }
        if (request.protocol() == null || request.protocol().isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("protocol is required").build());
        }
        return Optional.empty();
    }

    private Optional<Response> guard(String tenantId, String pipelineId, String authorization) {
        if (!enabled()) {
            return Optional.of(Response.status(Response.Status.NOT_FOUND).build());
        }
        if (tenantId == null || tenantId.isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("tenantId is required").build());
        }
        if (pipelineId == null || pipelineId.isBlank()) {
            return Optional.of(Response.status(Response.Status.BAD_REQUEST).entity("pipelineId is required").build());
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
                .entity("Worker lifecycle admin token is unavailable").build());
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

    private PipelineWorkerRegistry registry() {
        if (workerRegistry != null) {
            return workerRegistry;
        }
        PipelineWorkerRegistry fallback = fallbackRegistry;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackRegistry;
                if (fallback == null) {
                    fallback = new InMemoryPipelineWorkerRegistry();
                    fallbackRegistry = fallback;
                }
            }
        }
        return fallback;
    }
}
