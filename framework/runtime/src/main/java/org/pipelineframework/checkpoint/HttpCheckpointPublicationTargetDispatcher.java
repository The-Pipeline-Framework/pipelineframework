package org.pipelineframework.checkpoint;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
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
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final int LOG_BODY_PREVIEW_LIMIT = 160;

    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(CONNECT_TIMEOUT)
        .build();

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
            .timeout(REQUEST_TIMEOUT)
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
                        + previewBodySuffix(response.body())));
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

    private String previewBodySuffix(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        String normalized = body.replaceAll("\\s+", " ").trim();
        if (normalized.length() <= LOG_BODY_PREVIEW_LIMIT) {
            return " (body preview: " + normalized + ")";
        }
        return " (body preview: "
            + normalized.substring(0, LOG_BODY_PREVIEW_LIMIT)
            + "...)";
    }
}
