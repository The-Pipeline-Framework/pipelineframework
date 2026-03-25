package org.pipelineframework.checkpoint;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import jakarta.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;

/**
 * HTTP dispatcher for runtime checkpoint publication targets.
 */
@ApplicationScoped
@Unremovable
public class HttpCheckpointPublicationTargetDispatcher implements CheckpointPublicationTargetDispatcher {

    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final String DEFAULT_IDEMPOTENCY_HEADER = "Idempotency-Key";

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Override
    public PublicationTargetKind kind() {
        return PublicationTargetKind.HTTP;
    }

    @Override
    public Uni<Void> dispatch(
        ResolvedCheckpointPublicationTarget target,
        CheckpointPublicationRequest request,
        String tenantId,
        String idempotencyKey
    ) {
        byte[] body;
        try {
            body = encodeBody(request, target.encoding(), tenantId, idempotencyKey);
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(target.endpoint()))
            .header("Content-Type", target.contentType())
            .header("Accept", ProtobufHttpContentTypes.APPLICATION_JSON)
            .method(target.method(), HttpRequest.BodyPublishers.ofByteArray(body));
        if (tenantId != null && !tenantId.isBlank()) {
            builder.header("x-tenant-id", tenantId.trim());
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            builder.header(
                target.idempotencyHeader() == null || target.idempotencyHeader().isBlank()
                    ? DEFAULT_IDEMPOTENCY_HEADER
                    : target.idempotencyHeader(),
                idempotencyKey.trim());
        }

        return Uni.createFrom().completionStage(
            httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString()))
            .onItem().transformToUni(response -> {
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    return Uni.createFrom().voidItem();
                }
                return Uni.createFrom().failure(new IllegalStateException(
                    "Checkpoint publication target '" + target.targetId()
                        + "' rejected publication '" + target.publication()
                        + "' with status " + status
                        + " and body: " + response.body()));
            });
    }

    private byte[] encodeBody(
        CheckpointPublicationRequest request,
        PublicationEncoding encoding,
        String tenantId,
        String idempotencyKey
    ) throws IOException {
        if (encoding == PublicationEncoding.PROTO) {
            return CheckpointPublicationProtoSupport.toProtoRequest(request, tenantId, idempotencyKey).toByteArray();
        }
        return JSON.writeValueAsBytes(request);
    }
}
