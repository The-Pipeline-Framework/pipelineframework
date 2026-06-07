package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
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
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.dto.AwaitDtoMapper;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.dto.HostedAwaitCompletionRequest;
import org.pipelineframework.orchestrator.dto.HostedExecutionResultResponse;
import org.pipelineframework.orchestrator.dto.HostedExecutionSubmitRequest;

/**
 * Default-disabled generic control-plane API for local/dev hosted-coordinator proof.
 */
@ApplicationScoped
@Path("/tpf/control-plane/tenants/{tenantId}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class HostedPipelineControlPlaneResource {

    private static final Logger LOG = Logger.getLogger(HostedPipelineControlPlaneResource.class);
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String INVALID_REQUEST_PAYLOAD = "Invalid request payload";
    private static final String INGRESS_TYPE_UNAVAILABLE = "Hosted control-plane ingress payload type is unavailable";
    private static final int DEFAULT_PENDING_LIMIT = 100;
    private static final int MIN_PENDING_LIMIT = 1;
    private static final int MAX_PENDING_LIMIT = 1000;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineControlPlane controlPlane;

    @Inject
    PipelineExecutionService executionService;

    @Inject
    PipelineReleaseRegistry releaseRegistry;

    @Inject
    PipelineReleaseRegistrar releaseRegistrar;

    @Inject
    PipelineWorkerAvailability workerAvailability;

    @Inject
    TransitionPayloadCodec payloadCodec;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    private volatile PipelineReleaseRegistry fallbackRegistry;
    private volatile PipelineReleaseRegistrar fallbackRegistrar;
    private volatile PipelineWorkerAvailability fallbackWorkerAvailability;

    @PostConstruct
    void validateConfig() {
        if (!enabled()) {
            return;
        }
        WorkerSecretSupport.validationError(
            orchestratorConfig.controlPlane().adminToken(),
            orchestratorConfig.controlPlane().adminTokenRef(),
            "pipeline.orchestrator.control-plane.admin-token",
            "pipeline.orchestrator.control-plane.admin-token-ref",
            "pipeline.orchestrator.control-plane.enabled=true")
            .ifPresent(message -> {
                throw new IllegalStateException(message);
            });
    }

    @POST
    @Path("/executions")
    @Blocking
    public Uni<Response> submitExecution(
        @PathParam("tenantId") String tenantId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization,
        HostedExecutionSubmitRequest request
    ) {
        Response guard = guard(tenantId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        if (request == null || request.pipelineId() == null || request.pipelineId().isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("pipelineId is required").build());
        }
        if (request.inputShape() == null) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("inputShape is required").build());
        }
        if (request.inputPayload() == null) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("inputPayload is required").build());
        }
        return registry().active(tenantId, request.pipelineId())
            .onItem().transformToUni(active -> {
                if (active.isEmpty()) {
                    return Uni.createFrom().item(Response.status(Response.Status.CONFLICT)
                        .entity("No active release is registered for the requested pipeline").build());
                }
                PipelineReleaseRecord release = active.get();
                try {
                    registrar().verify(release);
                } catch (IllegalArgumentException | IllegalStateException e) {
                    LOG.warnf("Active hosted release is unavailable: %s", e.getMessage());
                    return Uni.createFrom().item(Response.status(Response.Status.CONFLICT)
                        .entity(e.getMessage()).build());
                }
                Object input;
                try {
                    input = executionInput(request, release);
                } catch (IngressPayloadTypeResolutionException e) {
                    LOG.errorf(e, "Failed resolving hosted control-plane ingress payload type");
                    return Uni.createFrom().item(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity(INGRESS_TYPE_UNAVAILABLE).build());
                } catch (RuntimeException e) {
                    LOG.warnf(e, "Invalid hosted control-plane execution submit payload");
                    return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                        .entity(INVALID_REQUEST_PAYLOAD).build());
                }
                return availability().check(new PipelineWorkerAvailabilityRequest(
                        tenantId,
                        release.pipelineId(),
                        release.contractVersion(),
                        release.releaseVersion(),
                        release.bundleVersionId()))
                    .onItem().transformToUni(availability -> {
                        if (!availability.available()) {
                            LOG.warnf(
                                "No %s transition worker is available for tenant=%s pipelineId=%s releaseVersion=%s: %s",
                                availability.providerName(),
                                tenantId,
                                release.pipelineId(),
                                release.releaseVersion(),
                                availability.message());
                            return Uni.createFrom().item(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                                .entity("No worker available for active release: " + availability.message()).build());
                        }
                        return controlPlane.executePipelineAsync(
                                input,
                                tenantId,
                                request.idempotencyKey(),
                                request.outputStreaming(),
                                release.pipelineId(),
                                release.contractVersion(),
                                release.releaseVersion(),
                                release.bundleVersionId())
                            .onItem().transform(accepted -> Response.ok(accepted).build());
                    });
            });
    }

    @GET
    @Path("/executions/{executionId}")
    @Blocking
    public Uni<Response> getExecutionStatus(
        @PathParam("tenantId") String tenantId,
        @PathParam("executionId") String executionId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization
    ) {
        Response guard = guard(tenantId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return controlPlane.getExecutionStatus(tenantId, executionId)
            .onItem().transform(status -> Response.ok(status).build());
    }

    @GET
    @Path("/executions/{executionId}/result")
    @Blocking
    public Uni<Response> getExecutionResult(
        @PathParam("tenantId") String tenantId,
        @PathParam("executionId") String executionId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization
    ) {
        Response guard = guard(tenantId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        return controlPlane.getExecutionStatus(tenantId, executionId)
            .onItem().transformToUni(status -> {
                if (status.status() != ExecutionStatus.SUCCEEDED) {
                    return Uni.createFrom().item(Response.ok(new HostedExecutionResultResponse(status, null)).build());
                }
                return controlPlane.getExecutionResultPayload(tenantId, executionId)
                    .onItem().transform(payload -> Response.ok(new HostedExecutionResultResponse(
                        status,
                        resultPayload(payload))).build());
            });
    }

    private SerializedTransitionPayload resultPayload(Object payload) {
        if (payload instanceof SerializedTransitionPayload serialized) {
            return serialized;
        }
        return payloadCodec.encode(payload);
    }

    @GET
    @Path("/interactions/pending")
    @Blocking
    public Uni<Response> pendingInteractions(
        @PathParam("tenantId") String tenantId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization,
        @QueryParam("assignee") String assignee,
        @QueryParam("group") String group,
        @QueryParam("stepId") String stepId,
        @QueryParam("limit") Integer limit
    ) {
        Response guard = guard(tenantId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        int effectiveLimit = clampPendingLimit(limit);
        return controlPlane.queryPendingAwaitInteractions(tenantId, assignee, group, stepId, effectiveLimit)
            .onItem().transform(interactions -> Response.ok(interactions.stream()
                .map(AwaitDtoMapper::toDto)
                .toList()).build());
    }

    @POST
    @Path("/interactions/complete")
    @Blocking
    public Uni<Response> completeInteraction(
        @PathParam("tenantId") String tenantId,
        @HeaderParam(AUTHORIZATION_HEADER) String authorization,
        HostedAwaitCompletionRequest request
    ) {
        Response guard = guard(tenantId, authorization);
        if (guard != null) {
            return Uni.createFrom().item(guard);
        }
        if (request == null) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("Await completion request is required").build());
        }
        if (isBlank(request.interactionId()) && isBlank(request.correlationId()) && isBlank(request.resumeToken())) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("interactionId, correlationId, or resumeToken is required").build());
        }
        if (request.responsePayload() == null) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity("responsePayload is required").build());
        }
        Object responsePayload;
        try {
            responsePayload = request.responsePayload() == null
                ? null
                : payloadCodec.decode(request.responsePayload());
        } catch (RuntimeException e) {
            LOG.warnf(e, "Invalid hosted control-plane await completion payload");
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                .entity(INVALID_REQUEST_PAYLOAD).build());
        }
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            tenantId,
            request.interactionId(),
            request.correlationId(),
            request.resumeToken(),
            request.idempotencyKey(),
            responsePayload,
            request.actor(),
            System.currentTimeMillis());
        return executionService.completeAwaitInteraction(command)
            .onItem().transform(result -> Response.ok(AwaitDtoMapper.toCompletionResponse(result)).build());
    }

    private Object executionInput(HostedExecutionSubmitRequest request, PipelineReleaseRecord release) {
        if (request == null) {
            throw new IllegalArgumentException("Execution submit request is required");
        }
        if (request.inputShape() == null) {
            throw new IllegalArgumentException("inputShape is required");
        }
        if (request.inputPayload() == null) {
            throw new IllegalArgumentException("inputPayload is required");
        }
        Object payload = payloadCodec.decode(request.inputPayload());
        return switch (request.inputShape()) {
            case UNI -> toUni(coerceIngressPayload(payload, request.inputPayload(), release));
            case MULTI -> toMulti(payload, release);
            case RAW -> payload;
        };
    }

    private Uni<?> toUni(Object payload) {
        return payload == null ? Uni.createFrom().nullItem() : Uni.createFrom().item(payload);
    }

    private Multi<?> toMulti(Object payload, PipelineReleaseRecord release) {
        if (payload == null) {
            return Multi.createFrom().empty();
        }
        if (payload instanceof Iterable<?> iterable) {
            List<Object> coerced = new ArrayList<>();
            iterable.forEach(item -> coerced.add(coerceIngressPayload(item, null, release)));
            return Multi.createFrom().iterable(coerced);
        }
        throw new IllegalArgumentException("MULTI input payload must decode to an iterable value");
    }

    private Object coerceIngressPayload(
        Object payload,
        SerializedTransitionPayload serializedPayload,
        PipelineReleaseRecord release
    ) {
        if (hasExplicitConcretePayloadType(serializedPayload)) {
            return payload;
        }
        Class<?> target = ingressPayloadType(release);
        if (target == null || payload == null || target.isInstance(payload)) {
            return payload;
        }
        return PipelineJson.mapper().convertValue(payload, target);
    }

    private boolean hasExplicitConcretePayloadType(SerializedTransitionPayload serializedPayload) {
        if (serializedPayload == null || serializedPayload.payloadTypeId() == null) {
            return false;
        }
        return switch (serializedPayload.payloadTypeId()) {
            case "null", "java.util.Map", "java.util.List", "java.util.Set" -> false;
            default -> true;
        };
    }

    private Class<?> ingressPayloadType(PipelineReleaseRecord release) {
        try {
            if (release == null || release.manifest() == null || release.manifest().steps().isEmpty()) {
                return null;
            }
            String inputTypeId = release.manifest().steps().getFirst().inputTypeId();
            if (inputTypeId == null || inputTypeId.isBlank()) {
                return null;
            }
            return loadClass(inputTypeId);
        } catch (Exception e) {
            throw new IngressPayloadTypeResolutionException(e);
        }
    }

    private Class<?> loadClass(String className) throws ClassNotFoundException {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        if (contextLoader != null) {
            try {
                return Class.forName(className, false, contextLoader);
            } catch (ClassNotFoundException ignored) {
                // Fall back below.
            }
        }
        return Class.forName(className, false, HostedPipelineControlPlaneResource.class.getClassLoader());
    }

    private Response guard(String tenantId, String authorization) {
        if (!enabled()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        if (tenantId == null || tenantId.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("tenantId is required").build();
        }
        return authenticate(authorization);
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

    private PipelineWorkerAvailability availability() {
        if (workerAvailability != null) {
            return workerAvailability;
        }
        PipelineWorkerAvailability fallback = fallbackWorkerAvailability;
        if (fallback == null) {
            synchronized (this) {
                fallback = fallbackWorkerAvailability;
                if (fallback == null) {
                    fallback = new DefaultPipelineWorkerAvailability();
                    fallbackWorkerAvailability = fallback;
                }
            }
        }
        return fallback;
    }

    private int clampPendingLimit(Integer limit) {
        int requested = limit == null ? DEFAULT_PENDING_LIMIT : limit;
        return Math.max(MIN_PENDING_LIMIT, Math.min(MAX_PENDING_LIMIT, requested));
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
                orchestratorConfig.controlPlane().adminToken(),
                orchestratorConfig.controlPlane().adminTokenRef(),
                secretResolver,
                "pipeline.orchestrator.control-plane.admin-token",
                "pipeline.orchestrator.control-plane.admin-token-ref");
        } catch (RuntimeException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("Hosted control-plane admin token is unavailable").build();
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
            && orchestratorConfig.controlPlane() != null
            && orchestratorConfig.controlPlane().enabled();
    }

    private static final class IngressPayloadTypeResolutionException extends RuntimeException {
        private IngressPayloadTypeResolutionException(Throwable cause) {
            super(cause);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
