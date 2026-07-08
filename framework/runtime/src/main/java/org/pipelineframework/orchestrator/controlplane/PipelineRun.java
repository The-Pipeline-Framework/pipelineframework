package org.pipelineframework.orchestrator.controlplane;

import java.util.Objects;
import org.pipelineframework.orchestrator.ExecutionResultShape;

public record PipelineRun(
    String tenantId,
    String runId,
    String executionKey,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    ExecutionResultShape resultShape,
    PipelineRunStatus status,
    Object inputPayload,
    Object resultPayload,
    String errorCode,
    String errorMessage,
    long version,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS
) {
    public PipelineRun {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        executionKey = ControlPlaneChecks.requireText(executionKey, "executionKey");
        pipelineId = ControlPlaneChecks.requireText(pipelineId, "pipelineId");
        contractVersion = ControlPlaneChecks.requireText(contractVersion, "contractVersion");
        releaseVersion = ControlPlaneChecks.requireText(releaseVersion, "releaseVersion");
        Objects.requireNonNull(resultShape, "resultShape must not be null");
        Objects.requireNonNull(status, "status must not be null");
        inputPayload = ControlPlaneChecks.freezePayload(inputPayload);
        resultPayload = ControlPlaneChecks.freezePayload(resultPayload);
        ControlPlaneChecks.requireNonNegative(version, "version");
        ControlPlaneChecks.requireNonNegative(createdAtEpochMs, "createdAtEpochMs");
        ControlPlaneChecks.requireNonNegative(updatedAtEpochMs, "updatedAtEpochMs");
        ControlPlaneChecks.requireNonNegative(ttlEpochS, "ttlEpochS");
    }

    PipelineRun withStatus(PipelineRunStatus nextStatus, long version, long nowEpochMs) {
        return new PipelineRun(
            tenantId,
            runId,
            executionKey,
            pipelineId,
            contractVersion,
            releaseVersion,
            resultShape,
            nextStatus,
            inputPayload,
            resultPayload,
            errorCode,
            errorMessage,
            version,
            createdAtEpochMs,
            nowEpochMs,
            ttlEpochS);
    }

    PipelineRun succeeded(Object resultPayload, long version, long nowEpochMs) {
        return new PipelineRun(
            tenantId,
            runId,
            executionKey,
            pipelineId,
            contractVersion,
            releaseVersion,
            resultShape,
            PipelineRunStatus.SUCCEEDED,
            inputPayload,
            resultPayload,
            null,
            null,
            version,
            createdAtEpochMs,
            nowEpochMs,
            ttlEpochS);
    }

    PipelineRun failed(String errorCode, String errorMessage, long version, long nowEpochMs) {
        errorCode = ControlPlaneChecks.requireText(errorCode, "errorCode");
        errorMessage = errorMessage == null ? "" : errorMessage;
        return new PipelineRun(
            tenantId,
            runId,
            executionKey,
            pipelineId,
            contractVersion,
            releaseVersion,
            resultShape,
            PipelineRunStatus.FAILED,
            inputPayload,
            resultPayload,
            errorCode,
            errorMessage,
            version,
            createdAtEpochMs,
            nowEpochMs,
            ttlEpochS);
    }
}
