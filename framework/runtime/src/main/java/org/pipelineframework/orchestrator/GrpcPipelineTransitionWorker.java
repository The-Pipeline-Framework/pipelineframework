package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.invocation.TransportBoundaryDescriptor;
import org.pipelineframework.invocation.TransportBoundaryInvocation;
import org.pipelineframework.orchestrator.grpc.MutinyTransitionWorkerServiceGrpc;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesResponse;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerResponse;

/**
 * gRPC client adapter for transition workers.
 */
@ApplicationScoped
public class GrpcPipelineTransitionWorker implements PipelineTransitionWorker, TransportBoundaryInvocation {

    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final TransportBoundaryDescriptor BOUNDARY =
        new TransportBoundaryDescriptor("grpc", "transition-worker.execute");

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    private volatile ManagedChannel channel;
    private volatile MutinyTransitionWorkerServiceGrpc.MutinyTransitionWorkerServiceStub stub;

    public GrpcPipelineTransitionWorker() {
    }

    GrpcPipelineTransitionWorker(PipelineInvocationRuntime invocationRuntime) {
        this.invocationRuntime = invocationRuntime;
    }

    GrpcPipelineTransitionWorker(ManagedChannel channel) {
        this(channel, null);
    }

    GrpcPipelineTransitionWorker(ManagedChannel channel, PipelineInvocationRuntime invocationRuntime) {
        this.channel = channel;
        this.stub = MutinyTransitionWorkerServiceGrpc.newMutinyStub(channel);
        this.invocationRuntime = invocationRuntime;
    }

    @Override
    public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
        return invocationRuntime().invokeTransportUni(this, () ->
            Uni.createFrom().item(() -> request(command))
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .chain(request -> stub().execute(request))
                .ifNoItem().after(orchestratorConfig.workerGrpc().requestTimeout())
                .fail()
                .onItem().transformToUni(response -> Uni.createFrom().item(() -> decodeResponse(response, command))
                    .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                .onFailure().transform(failure -> failure instanceof TransitionWorkerFailureException
                    ? failure
                    : new TransitionWorkerFailureException(
                        "gRPC transition worker failed for execution " + command.executionId(), failure)));
    }

    /**
     * Fetches worker bundle capabilities from the gRPC worker.
     *
     * @return remote worker capabilities
     */
    public Uni<PipelineWorkerCapability> capabilities() {
        return Uni.createFrom().item(this::capabilitiesRequest)
            .runSubscriptionOn(Infrastructure.getDefaultExecutor())
            .chain(request -> stub().capabilities(request))
            .ifNoItem().after(orchestratorConfig.workerGrpc().requestTimeout())
            .fail()
            .onItem().transformToUni(response -> Uni.createFrom().item(() -> decodeCapabilitiesResponse(response))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
            .onFailure().transform(failure -> failure instanceof TransitionWorkerFailureException
                ? failure
                : new TransitionWorkerFailureException("gRPC transition worker capability check failed", failure));
    }

    @Override
    public String providerName() {
        return "grpc";
    }

    @Override
    public TransportBoundaryDescriptor transportBoundary() {
        return BOUNDARY;
    }

    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (!config.workerGrpc().isEnabled()) {
            return Optional.empty();
        }
        Optional<String> secretError = WorkerSecretSupport.validationError(
            config.workerGrpc().sharedSecret(),
            config.workerGrpc().sharedSecretRef(),
            "pipeline.orchestrator.worker.grpc.shared-secret",
            "pipeline.orchestrator.worker.grpc.shared-secret-ref",
            "the gRPC transition worker is enabled");
        if (secretError.isPresent()) {
            return secretError;
        }
        return config.workerGrpc().endpoint().flatMap(GrpcPipelineTransitionWorker::validateEndpoint);
    }

    private TransitionWorkerRequest request(TransitionCommandEnvelope command) {
        try {
            byte[] body = JSON.writeValueAsBytes(command);
            String timestamp = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String signature = TransitionWorkerSignature.sign(
                sharedSecret(),
                GrpcTransitionWorkerProtocol.SIGNATURE_METHOD,
                GrpcTransitionWorkerProtocol.SIGNATURE_PATH,
                timestamp,
                nonce,
                body);
            return TransitionWorkerRequest.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .setCommandEnvelope(ByteString.copyFrom(body))
                .setTimestamp(timestamp)
                .setNonce(nonce)
                .setSignature(signature)
                .build();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding gRPC transition worker command", e);
        }
    }

    private TransitionWorkerCapabilitiesRequest capabilitiesRequest() {
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            sharedSecret(),
            GrpcTransitionWorkerProtocol.SIGNATURE_METHOD,
            GrpcTransitionWorkerProtocol.CAPABILITIES_SIGNATURE_PATH,
            timestamp,
            nonce,
            new byte[0]);
        return TransitionWorkerCapabilitiesRequest.newBuilder()
            .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
            .setTimestamp(timestamp)
            .setNonce(nonce)
            .setSignature(signature)
            .build();
    }

    private TransitionResultEnvelope decodeResponse(TransitionWorkerResponse response, TransitionCommandEnvelope command) {
        if (!GrpcTransitionWorkerProtocol.PROTOCOL_VERSION.equals(response.getProtocolVersion())) {
            throw new TransitionWorkerFailureException(
                "gRPC transition worker returned unsupported protocol version " + response.getProtocolVersion()
                    + " for execution " + command.executionId());
        }
        if (!GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING.equals(response.getPayloadEncoding())) {
            throw new TransitionWorkerFailureException(
                "gRPC transition worker returned unsupported payload encoding " + response.getPayloadEncoding()
                    + " for execution " + command.executionId());
        }
        try {
            return JSON.readValue(response.getResultEnvelope().toByteArray(), TransitionResultEnvelope.class);
        } catch (IOException e) {
            throw new TransitionWorkerFailureException(
                "gRPC transition worker returned malformed JSON for execution " + command.executionId(),
                e);
        }
    }

    private PipelineWorkerCapability decodeCapabilitiesResponse(TransitionWorkerCapabilitiesResponse response) {
        if (!GrpcTransitionWorkerProtocol.PROTOCOL_VERSION.equals(response.getProtocolVersion())) {
            throw new TransitionWorkerFailureException(
                "gRPC transition worker capabilities returned unsupported protocol version "
                    + response.getProtocolVersion());
        }
        if (!GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING.equals(response.getPayloadEncoding())) {
            throw new TransitionWorkerFailureException(
                "gRPC transition worker capabilities returned unsupported payload encoding "
                    + response.getPayloadEncoding());
        }
        try {
            return JSON.readValue(response.getCapabilityEnvelope().toByteArray(), PipelineWorkerCapability.class);
        } catch (IOException e) {
            throw new TransitionWorkerFailureException(
                "gRPC transition worker capabilities returned malformed JSON",
                e);
        }
    }

    private MutinyTransitionWorkerServiceGrpc.MutinyTransitionWorkerServiceStub stub() {
        MutinyTransitionWorkerServiceGrpc.MutinyTransitionWorkerServiceStub active = stub;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (stub == null) {
                channel = channel();
                stub = MutinyTransitionWorkerServiceGrpc.newMutinyStub(channel);
            }
            return stub;
        }
    }

    private ManagedChannel channel() {
        ManagedChannel active = channel;
        if (active != null) {
            return active;
        }
        String endpoint = orchestratorConfig.workerGrpc().endpoint()
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.worker.grpc.endpoint is required for gRPC transition worker"));
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(endpoint);
        if (orchestratorConfig.workerGrpc().plaintext()) {
            builder.usePlaintext();
        }
        builder.maxInboundMessageSize(Math.max(1024, orchestratorConfig.workerGrpc().maxInboundMessageSize()));
        return builder.build();
    }

    private String sharedSecret() {
        return WorkerSecretSupport.resolve(
            orchestratorConfig.workerGrpc().sharedSecret(),
            orchestratorConfig.workerGrpc().sharedSecretRef(),
            secretResolver,
            "pipeline.orchestrator.worker.grpc.shared-secret",
            "pipeline.orchestrator.worker.grpc.shared-secret-ref");
    }

    private PipelineInvocationRuntime invocationRuntime() {
        if (invocationRuntime == null) {
            throw new IllegalStateException("PipelineInvocationRuntime was not injected into "
                + "GrpcPipelineTransitionWorker.invocationRuntime");
        }
        return invocationRuntime;
    }

    @PreDestroy
    void closeChannel() {
        ManagedChannel active = channel;
        if (active == null) {
            return;
        }
        active.shutdown();
        try {
            if (!active.awaitTermination(5, TimeUnit.SECONDS)) {
                active.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            active.shutdownNow();
        } finally {
            channel = null;
            stub = null;
        }
    }

    private static Optional<String> validateEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return Optional.of("pipeline.orchestrator.worker.grpc.endpoint must not be blank");
        }
        String trimmed = endpoint.trim();
        if (trimmed.contains("://")) {
            try {
                URI uri = URI.create(trimmed);
                if (uri.getScheme() == null || uri.getScheme().isBlank()
                    || uri.getSchemeSpecificPart() == null || uri.getSchemeSpecificPart().isBlank()) {
                    return Optional.of("pipeline.orchestrator.worker.grpc.endpoint URI target is incomplete");
                }
                return Optional.empty();
            } catch (IllegalArgumentException e) {
                return Optional.of("pipeline.orchestrator.worker.grpc.endpoint URI target is invalid");
            }
        }
        if (trimmed.startsWith("[")) {
            int bracket = trimmed.indexOf(']');
            if (bracket <= 1 || bracket + 2 > trimmed.length() || trimmed.charAt(bracket + 1) != ':') {
                return Optional.of("pipeline.orchestrator.worker.grpc.endpoint must be host:port");
            }
            return validatePort(trimmed.substring(bracket + 2));
        }
        int separator = trimmed.lastIndexOf(':');
        if (separator <= 0 || separator == trimmed.length() - 1 || trimmed.indexOf(':') != separator) {
            return Optional.of("pipeline.orchestrator.worker.grpc.endpoint must be host:port");
        }
        String host = trimmed.substring(0, separator);
        String portValue = trimmed.substring(separator + 1);
        if (host.isBlank()) {
            return Optional.of("pipeline.orchestrator.worker.grpc.endpoint host must not be blank");
        }
        return validatePort(portValue);
    }

    private static Optional<String> validatePort(String portValue) {
        try {
            int port = Integer.parseInt(portValue);
            if (port < 1 || port > 65535) {
                return Optional.of("pipeline.orchestrator.worker.grpc.endpoint port must be between 1 and 65535");
            }
        } catch (NumberFormatException e) {
            return Optional.of("pipeline.orchestrator.worker.grpc.endpoint port must be numeric");
        }
        return Optional.empty();
    }
}
