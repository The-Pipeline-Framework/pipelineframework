package org.pipelineframework.checkpoint;

import java.io.IOException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishAcceptedResponse;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.checkpoint.grpc.MutinyCheckpointPublicationServiceGrpc;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.telemetry.RpcMetrics;

/**
 * Generic gRPC admission endpoint for checkpoint publications.
 */
@GrpcService
@Singleton
public class CheckpointPublicationGrpcService
    extends MutinyCheckpointPublicationServiceGrpc.CheckpointPublicationServiceImplBase {

    static final String SERVICE = "CheckpointPublicationService";
    static final String METHOD = "Publish";

    private static final Logger LOG = Logger.getLogger(CheckpointPublicationGrpcService.class);

    @Inject
    CheckpointPublicationAdmissionService admissionService;

    @Override
    public Uni<CheckpointPublishAcceptedResponse> publish(CheckpointPublishRequest request) {
        long startTime = System.nanoTime();
        CheckpointPublicationRequest decoded;
        try {
            decoded = CheckpointPublicationProtoSupport.fromProtoRequest(request);
        } catch (IllegalArgumentException | IOException e) {
            StatusRuntimeException failure = statusFailure(Status.INVALID_ARGUMENT, e.getMessage(), e);
            RpcMetrics.recordGrpcServer(SERVICE, METHOD, failure.getStatus(), System.nanoTime() - startTime);
            return Uni.createFrom().failure(failure);
        }

        return Uni.createFrom().deferred(
                () -> admissionService.admit(decoded, request.getTenantId(), request.getIdempotencyKey()))
            .onItem().transform(CheckpointPublicationProtoSupport::toProtoResponse)
            .onItem().invoke(response ->
                RpcMetrics.recordGrpcServer(SERVICE, METHOD, Status.OK, System.nanoTime() - startTime))
            .onFailure().transform(this::mapFailure)
            .onFailure().invoke(failure ->
                RpcMetrics.recordGrpcServer(SERVICE, METHOD, Status.fromThrowable(failure),
                    System.nanoTime() - startTime));
    }

    private Throwable mapFailure(Throwable failure) {
        if (failure instanceof StatusRuntimeException) {
            return failure;
        }
        if (failure instanceof IllegalArgumentException) {
            return statusFailure(Status.INVALID_ARGUMENT, failure.getMessage(), failure);
        }
        if (failure instanceof jakarta.ws.rs.NotFoundException) {
            return statusFailure(Status.NOT_FOUND, failure.getMessage(), failure);
        }
        LOG.debugf(failure, "Checkpoint gRPC publication failed: %s", failure.getMessage());
        return statusFailure(Status.INTERNAL, failure.getMessage(), failure);
    }

    private StatusRuntimeException statusFailure(Status status, String message, Throwable cause) {
        return status.withDescription(message == null ? status.getCode().name() : message)
            .withCause(cause)
            .asRuntimeException();
    }
}
