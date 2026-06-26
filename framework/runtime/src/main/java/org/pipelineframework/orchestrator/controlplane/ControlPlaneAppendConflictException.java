package org.pipelineframework.orchestrator.controlplane;

public class ControlPlaneAppendConflictException extends RuntimeException {

    public ControlPlaneAppendConflictException(String message) {
        super(message);
    }
}
