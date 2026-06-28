package org.pipelineframework;

import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;

record PipelineRunSubmission(
    String tenantId,
    boolean explicitTenant,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String idempotencyKey,
    boolean outputStreaming) {

  ControlPlaneAdmissionRequest admissionRequest() {
    return new ControlPlaneAdmissionRequest(
        tenantId,
        ControlPlaneAdmissionOperation.SUBMIT_EXECUTION,
        pipelineId,
        releaseVersion,
        null,
        "api",
        explicitTenant);
  }
}
