package org.pipelineframework.orchestrator;

/**
 * Permit for a tenant-scoped transition admission.
 */
@FunctionalInterface
public interface ControlPlaneTransitionPermit extends AutoCloseable {

    ControlPlaneTransitionPermit NOOP = () -> {
    };

    /**
     * Release the transition permit.
     */
    @Override
    void close();

    /**
     * No-op transition permit.
     *
     * @return no-op permit
     */
    static ControlPlaneTransitionPermit noop() {
        return NOOP;
    }
}
