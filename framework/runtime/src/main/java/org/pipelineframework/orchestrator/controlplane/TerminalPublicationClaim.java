package org.pipelineframework.orchestrator.controlplane;

import java.util.Objects;

public record TerminalPublicationClaim(
    Status status,
    String publicationId,
    String idempotencyKey,
    String preparedFactKey,
    String completedFactKey
) {

    public enum Status {
        UNTRACKED,
        CLAIMED,
        PREPARED_RETRY,
        ALREADY_COMPLETED
    }

    public TerminalPublicationClaim {
        Objects.requireNonNull(status, "status must not be null");
        publicationId = requireText(publicationId, "publicationId");
        idempotencyKey = requireText(idempotencyKey, "idempotencyKey");
        if (status == Status.UNTRACKED) {
            preparedFactKey = null;
            completedFactKey = null;
        } else {
            preparedFactKey = requireText(preparedFactKey, "preparedFactKey");
            completedFactKey = requireText(completedFactKey, "completedFactKey");
        }
    }

    public static TerminalPublicationClaim untracked(String publicationId, String idempotencyKey) {
        return new TerminalPublicationClaim(Status.UNTRACKED, publicationId, idempotencyKey, null, null);
    }

    public boolean shouldPublish() {
        return status != Status.ALREADY_COMPLETED;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
