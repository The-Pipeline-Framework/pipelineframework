package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;
import java.io.IOException;
import java.util.List;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.common.annotation.Blocking;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.grpc.MutinyTransitionWorkerServiceGrpc;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesResponse;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerResponse;

/**
 * Default-disabled gRPC service for executing transition worker commands.
 */
@GrpcService
@Singleton
public class GrpcTransitionWorkerService
    extends MutinyTransitionWorkerServiceGrpc.TransitionWorkerServiceImplBase {

    private static final ObjectMapper JSON = PipelineJson.mapper();

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineExecutionService executionService;

    @Inject
    PipelineReleaseIdentityResolver identityResolver;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    private final TransitionWorkerNonceReplayGuard nonceReplayGuard = new TransitionWorkerNonceReplayGuard();

    @PostConstruct
    void validateServerConfig() {
        if (!orchestratorConfig.workerGrpc().serverEnabled()) {
            return;
        }
        WorkerSecretSupport.validationError(
            orchestratorConfig.workerGrpc().sharedSecret(),
            orchestratorConfig.workerGrpc().sharedSecretRef(),
            "pipeline.orchestrator.worker.grpc.shared-secret",
            "pipeline.orchestrator.worker.grpc.shared-secret-ref",
            "pipeline.orchestrator.worker.grpc.server-enabled=true")
            .ifPresent(message -> {
                throw new IllegalStateException(message);
            });
    }

    @Override
    @Blocking
    public Uni<TransitionWorkerResponse> execute(TransitionWorkerRequest request) {
        if (!orchestratorConfig.workerGrpc().serverEnabled()) {
            return Uni.createFrom().failure(Status.UNIMPLEMENTED
                .withDescription("gRPC transition worker service is disabled")
                .asRuntimeException());
        }
        Status authFailure = authenticate(
            request.getTimestamp(),
            request.getNonce(),
            request.getSignature(),
            request.getCommandEnvelope().toByteArray(),
            GrpcTransitionWorkerProtocol.SIGNATURE_PATH);
        if (authFailure != null) {
            return Uni.createFrom().failure(authFailure.asRuntimeException());
        }
        if (!GrpcTransitionWorkerProtocol.PROTOCOL_VERSION.equals(request.getProtocolVersion())
            || !GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING.equals(request.getPayloadEncoding())) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("Unsupported gRPC transition worker protocol envelope")
                .asRuntimeException());
        }
        TransitionCommandEnvelope envelope;
        try {
            envelope = JSON.readValue(request.getCommandEnvelope().toByteArray(), TransitionCommandEnvelope.class);
        } catch (IOException e) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("Malformed gRPC transition worker command envelope")
                .withCause(e)
                .asRuntimeException());
        }
        return executionService.executePortableTransition(envelope)
            .onItem().transform(this::response)
            .onFailure().transform(this::toGrpcFailure);
    }

    @Override
    @Blocking
    public Uni<TransitionWorkerCapabilitiesResponse> capabilities(TransitionWorkerCapabilitiesRequest request) {
        if (!orchestratorConfig.workerGrpc().serverEnabled()) {
            return Uni.createFrom().failure(Status.UNIMPLEMENTED
                .withDescription("gRPC transition worker service is disabled")
                .asRuntimeException());
        }
        Status authFailure = authenticate(
            request.getTimestamp(),
            request.getNonce(),
            request.getSignature(),
            new byte[0],
            GrpcTransitionWorkerProtocol.CAPABILITIES_SIGNATURE_PATH);
        if (authFailure != null) {
            return Uni.createFrom().failure(authFailure.asRuntimeException());
        }
        if (!GrpcTransitionWorkerProtocol.PROTOCOL_VERSION.equals(request.getProtocolVersion())) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("Unsupported gRPC transition worker capability protocol")
                .asRuntimeException());
        }
        return Uni.createFrom().item(this::capabilitiesResponse)
            .onFailure().transform(this::toGrpcFailure);
    }

    private RuntimeException toGrpcFailure(Throwable failure) {
        if (failure instanceof StatusRuntimeException statusFailure) {
            return statusFailure;
        }
        if (failure instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT
                .withDescription("Invalid gRPC transition worker command")
                .withCause(failure)
                .asRuntimeException();
        }
        if (failure instanceof IllegalStateException || failure instanceof TransitionWorkerFailureException) {
            return Status.FAILED_PRECONDITION
                .withDescription("Failed executing gRPC transition worker command")
                .withCause(failure)
                .asRuntimeException();
        }
        return Status.INTERNAL
            .withDescription("Failed executing gRPC transition worker command")
            .withCause(failure)
            .asRuntimeException();
    }

    private TransitionWorkerResponse response(TransitionResultEnvelope result) {
        try {
            byte[] body = JSON.writeValueAsBytes(result);
            return TransitionWorkerResponse.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .setResultEnvelope(ByteString.copyFrom(body))
                .build();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding gRPC transition worker result", e);
        }
    }

    private TransitionWorkerCapabilitiesResponse capabilitiesResponse() {
        try {
            byte[] body = JSON.writeValueAsBytes(capability());
            return TransitionWorkerCapabilitiesResponse.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .setCapabilityEnvelope(ByteString.copyFrom(body))
                .build();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding gRPC transition worker capabilities", e);
        }
    }

    private PipelineWorkerCapability capability() {
        PipelineBundleCapabilities capabilities = identityResolver.capabilities();
        return new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            "grpc",
            identityResolver.pipelineId(orchestratorConfig),
            identityResolver.contractVersion(),
            identityResolver.releaseVersion(orchestratorConfig),
            identityResolver.artifactId(orchestratorConfig),
            identityResolver.artifactDigest(orchestratorConfig),
            List.of(TransitionPayloadEncoding.JSON),
            capabilities.transitionWorkerProtocols());
    }

    private Status authenticate(
        String timestamp,
        String nonce,
        String signature,
        byte[] body,
        String signaturePath
    ) {
        String secret;
        try {
            secret = WorkerSecretSupport.resolve(
                orchestratorConfig.workerGrpc().sharedSecret(),
                orchestratorConfig.workerGrpc().sharedSecretRef(),
                secretResolver,
                "pipeline.orchestrator.worker.grpc.shared-secret",
                "pipeline.orchestrator.worker.grpc.shared-secret-ref");
        } catch (RuntimeException e) {
            return Status.FAILED_PRECONDITION
                .withDescription("gRPC transition worker shared secret is unavailable");
        }
        if (timestamp == null || timestamp.isBlank()
            || nonce == null || nonce.isBlank()
            || signature == null || signature.isBlank()) {
            return Status.UNAUTHENTICATED;
        }
        long timestampEpochMs;
        try {
            timestampEpochMs = TransitionWorkerSignature.parseTimestamp(timestamp);
        } catch (IllegalArgumentException e) {
            return Status.UNAUTHENTICATED;
        }
        long now = System.currentTimeMillis();
        long toleranceMs = Math.max(0L, orchestratorConfig.workerGrpc().signatureTolerance().toMillis());
        if (Math.abs(now - timestampEpochMs) > toleranceMs) {
            return Status.UNAUTHENTICATED;
        }
        String expected = TransitionWorkerSignature.sign(
            secret,
            GrpcTransitionWorkerProtocol.SIGNATURE_METHOD,
            signaturePath,
            timestamp,
            nonce,
            body);
        if (!TransitionWorkerSignature.matches(expected, signature)) {
            return Status.UNAUTHENTICATED;
        }
        if (!nonceReplayGuard.accept(nonce, timestampEpochMs, now, toleranceMs)) {
            return Status.UNAUTHENTICATED;
        }
        return null;
    }
}
