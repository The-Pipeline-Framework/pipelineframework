package org.pipelineframework.checkpoint;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.checkpoint.grpc.MutinyCheckpointPublicationServiceGrpc;
import org.pipelineframework.telemetry.GrpcClientTracing;

/**
 * gRPC dispatcher for runtime checkpoint publication targets.
 */
@ApplicationScoped
    @Unremovable
public class GrpcCheckpointPublicationTargetDispatcher implements CheckpointPublicationTargetDispatcher {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, MutinyCheckpointPublicationServiceGrpc.MutinyCheckpointPublicationServiceStub> stubs =
        new ConcurrentHashMap<>();

    @Override
    public PublicationTargetKind kind() {
        return PublicationTargetKind.GRPC;
    }

    @Override
    public Uni<Void> dispatch(
        ResolvedCheckpointPublicationTarget target,
        CheckpointPublicationRequest request,
        String tenantId,
        String idempotencyKey
    ) {
        try {
            return GrpcClientTracing.traceUnary(
                CheckpointPublicationGrpcService.SERVICE,
                CheckpointPublicationGrpcService.METHOD,
                stubFor(target).publish(CheckpointPublicationProtoSupport.toProtoRequest(request, tenantId, idempotencyKey)))
                .replaceWithVoid();
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    @PreDestroy
    void shutdown() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
        channels.clear();
        stubs.clear();
    }

    private MutinyCheckpointPublicationServiceGrpc.MutinyCheckpointPublicationServiceStub stubFor(
        ResolvedCheckpointPublicationTarget target
    ) {
        return stubs.computeIfAbsent(target.endpoint(),
            ignored -> MutinyCheckpointPublicationServiceGrpc.newMutinyStub(channelFor(target)));
    }

    private ManagedChannel channelFor(ResolvedCheckpointPublicationTarget target) {
        return channels.computeIfAbsent(target.endpoint(), ignored -> {
            String[] parts = target.endpoint().split(":", 2);
            if (parts.length != 2) {
                throw new IllegalStateException(
                    "Checkpoint gRPC target '" + target.targetId() + "' has invalid endpoint " + target.endpoint());
            }
            int port;
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                    "Checkpoint gRPC target '" + target.targetId() + "' has invalid port in " + target.endpoint(), e);
            }
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(parts[0], port);
            if ("PLAINTEXT".equals(target.method())) {
                builder.usePlaintext();
            }
            return builder.build();
        });
    }
}
