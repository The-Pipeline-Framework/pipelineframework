package org.pipelineframework.connector;

/** Whether Query observation metadata came from a provider call or a capture replay. */
public enum QueryObservationOrigin {
    LIVE_PROVIDER,
    CAPTURE_REPLAY
}
