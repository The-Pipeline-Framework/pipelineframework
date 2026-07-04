package org.pipelineframework.branching;

/**
 * Raised when runtime data reaches a branch-aware step with no valid route.
 */
public class PipelineBranchRoutingException extends RuntimeException {

    public PipelineBranchRoutingException(String message) {
        super(message);
    }
}
