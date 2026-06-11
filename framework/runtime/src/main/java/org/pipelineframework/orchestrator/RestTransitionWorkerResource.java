package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.smallrye.mutiny.Uni;
import io.smallrye.common.annotation.Blocking;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Default-disabled REST endpoint for executing transition worker commands.
 */
@ApplicationScoped
@Path("/pipeline/worker")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class RestTransitionWorkerResource {

    static final String RESOURCE_ROOT = "/pipeline/worker";
    static final String EXECUTE_PATH = RESOURCE_ROOT + "/transitions/execute";
    static final String CAPABILITIES_PATH = RESOURCE_ROOT + "/capabilities";

    private static final ObjectMapper JSON = PipelineJson.mapper();

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineExecutionService executionService;

    @Inject
    PipelineBundleIdentityResolver identityResolver;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    private final TransitionWorkerNonceReplayGuard nonceReplayGuard = new TransitionWorkerNonceReplayGuard();

    @PostConstruct
    void validateServerConfig() {
        if (!orchestratorConfig.workerRest().serverEnabled()) {
            return;
        }
        if (!EXECUTE_PATH.equals(orchestratorConfig.workerRest().path())) {
            throw new IllegalStateException("REST transition worker server only supports "
                + "pipeline.orchestrator.worker.rest.path=" + EXECUTE_PATH);
        }
        if (!CAPABILITIES_PATH.equals(orchestratorConfig.workerRest().capabilitiesPath())) {
            throw new IllegalStateException("REST transition worker server only supports "
                + "pipeline.orchestrator.worker.rest.capabilities-path=" + CAPABILITIES_PATH);
        }
        WorkerSecretSupport.validationError(
            orchestratorConfig.workerRest().sharedSecret(),
            orchestratorConfig.workerRest().sharedSecretRef(),
            "pipeline.orchestrator.worker.rest.shared-secret",
            "pipeline.orchestrator.worker.rest.shared-secret-ref",
            "pipeline.orchestrator.worker.rest.server-enabled=true")
            .ifPresent(message -> {
                throw new IllegalStateException(message);
            });
    }

    @POST
    @Path("/transitions/execute")
    @Blocking
    public Uni<Response> execute(
        @HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature,
        byte[] body
    ) {
        if (!orchestratorConfig.workerRest().serverEnabled()) {
            return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
        }
        Response authFailure = authenticate(
            "POST",
            EXECUTE_PATH,
            timestamp,
            nonce,
            signature,
            body);
        if (authFailure != null) {
            return Uni.createFrom().item(authFailure);
        }
        TransitionCommandEnvelope envelope;
        try {
            envelope = JSON.readValue(body == null ? new byte[0] : body, TransitionCommandEnvelope.class);
        } catch (IOException e) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST).build());
        }
        if (envelope == null) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST).build());
        }
        return executionService.executePortableTransition(envelope)
            .onItem().transform(result -> Response.ok(result).build());
    }

    @GET
    @Path("/capabilities")
    @Blocking
    public Uni<Response> capabilities(
        @HeaderParam(TransitionWorkerSignature.TIMESTAMP_HEADER) String timestamp,
        @HeaderParam(TransitionWorkerSignature.NONCE_HEADER) String nonce,
        @HeaderParam(TransitionWorkerSignature.SIGNATURE_HEADER) String signature
    ) {
        if (!orchestratorConfig.workerRest().serverEnabled()) {
            return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
        }
        Response authFailure = authenticate(
            "GET",
            CAPABILITIES_PATH,
            timestamp,
            nonce,
            signature,
            new byte[0]);
        if (authFailure != null) {
            return Uni.createFrom().item(authFailure);
        }
        return Uni.createFrom().item(Response.ok(capability()).build());
    }

    private PipelineWorkerCapability capability() {
        PipelineBundleCapabilities capabilities = identityResolver.capabilities();
        return new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            "rest",
            identityResolver.pipelineId(orchestratorConfig),
            identityResolver.bundleVersionId(orchestratorConfig),
            identityResolver.bundleHash(),
            List.of(TransitionPayloadEncoding.JSON),
            capabilities.transitionWorkerProtocols());
    }

    private Response authenticate(
        String method,
        String path,
        String timestamp,
        String nonce,
        String signature,
        byte[] body
    ) {
        String secret;
        try {
            secret = WorkerSecretSupport.resolve(
                orchestratorConfig.workerRest().sharedSecret(),
                orchestratorConfig.workerRest().sharedSecretRef(),
                secretResolver,
                "pipeline.orchestrator.worker.rest.shared-secret",
                "pipeline.orchestrator.worker.rest.shared-secret-ref");
        } catch (RuntimeException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("REST transition worker shared secret is unavailable").build();
        }
        if (timestamp == null || timestamp.isBlank()
            || nonce == null || nonce.isBlank()
            || signature == null || signature.isBlank()) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        long timestampEpochMs;
        try {
            timestampEpochMs = TransitionWorkerSignature.parseTimestamp(timestamp);
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        long now = System.currentTimeMillis();
        long toleranceMs = Math.max(0L, orchestratorConfig.workerRest().signatureTolerance().toMillis());
        if (Math.abs(now - timestampEpochMs) > toleranceMs) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        String expected = TransitionWorkerSignature.sign(
            secret,
            method,
            path,
            timestamp,
            nonce,
            body == null ? new byte[0] : body);
        if (!TransitionWorkerSignature.matches(expected, signature)) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        if (!nonceReplayGuard.accept(nonce, timestampEpochMs, now, toleranceMs)) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        return null;
    }
}
