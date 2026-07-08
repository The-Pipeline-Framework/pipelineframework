package org.pipelineframework.query;

import java.time.Instant;

/**
 * Captured output for a query step in one managed pipeline execution.
 */
public record QueryCaptureRecord(
    String tenantId,
    String executionId,
    int stepIndex,
    String queryId,
    String queryVersion,
    String captureKey,
    String inputJson,
    String outputJson,
    String outputType,
    Instant capturedAt
) {
    public QueryCaptureRecord {
        requireText(tenantId, "tenantId");
        requireText(executionId, "executionId");
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be non-negative");
        }
        requireText(queryId, "queryId");
        requireText(queryVersion, "queryVersion");
        requireText(captureKey, "captureKey");
        requireText(outputJson, "outputJson");
        requireText(outputType, "outputType");
        capturedAt = capturedAt == null ? Instant.now() : capturedAt;
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
