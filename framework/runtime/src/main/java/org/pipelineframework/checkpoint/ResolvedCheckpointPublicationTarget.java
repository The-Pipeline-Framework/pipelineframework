package org.pipelineframework.checkpoint;

/**
 * One resolved concrete runtime target for a logical publication.
 *
 * @param publication logical publication name
 * @param targetId configured target id
 * @param kind target kind
 * @param encoding payload encoding
 * @param contentType content type to send when applicable
 * @param idempotencyHeader idempotency header name to use when applicable
 * @param endpoint primary resolved endpoint string
 * @param method protocol-specific method or verb
 */
public record ResolvedCheckpointPublicationTarget(
    String publication,
    String targetId,
    PublicationTargetKind kind,
    PublicationEncoding encoding,
    String contentType,
    String idempotencyHeader,
    String endpoint,
    String method
) {
    public ResolvedCheckpointPublicationTarget {
        if (publication == null || publication.isBlank()) {
            throw new IllegalArgumentException("publication must not be blank");
        }
        if (kind == null) {
            throw new NullPointerException("kind must not be null");
        }
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("endpoint must not be blank");
        }
        if (method == null || method.isBlank()) {
            throw new IllegalArgumentException("method must not be blank");
        }
        if (kind == PublicationTargetKind.HTTP) {
            if (contentType == null || contentType.isBlank()) {
                throw new IllegalArgumentException("contentType must not be blank for HTTP targets");
            }
            if (!"POST".equals(method)) {
                throw new IllegalArgumentException("HTTP checkpoint publication targets must use method POST");
            }
        }
        if (kind == PublicationTargetKind.GRPC && !"PLAINTEXT".equals(method) && !"TLS".equals(method)) {
            throw new IllegalArgumentException("GRPC checkpoint publication targets must use method PLAINTEXT or TLS");
        }
    }
}
