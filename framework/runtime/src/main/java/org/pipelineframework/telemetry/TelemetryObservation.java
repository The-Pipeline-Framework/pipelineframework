package org.pipelineframework.telemetry;

import java.time.Instant;

/** Marker for granular runtime facts consumed by signal adapters and runtime safety. */
public sealed interface TelemetryObservation permits TelemetryObservation.RunStarted,
    TelemetryObservation.RunFinished, TelemetryObservation.StepCompleted,
    TelemetryObservation.TransitionRejected, TelemetryObservation.TransitionDispatched,
    TelemetryObservation.AwaitInteractionCreated, TelemetryObservation.AwaitProviderDispatched,
    TelemetryObservation.AwaitCompletionAdmitted, TelemetryObservation.AwaitLiveHandoff,
    TelemetryObservation.AwaitScalarContinuationStarted, TelemetryObservation.TerminalPublicationCompleted,
    TelemetryObservation.RetryScheduled {

    Instant occurredAt();

    record RunStarted(String pipeline, Instant occurredAt) implements TelemetryObservation { }
    record RunFinished(String pipeline, boolean successful, Instant occurredAt) implements TelemetryObservation { }
    record StepCompleted(String stepClass, String cardinality, boolean successful, Instant occurredAt) implements TelemetryObservation { }
    record TransitionRejected(String transition, String reason, Instant occurredAt) implements TelemetryObservation { }
    record TransitionDispatched(Instant occurredAt) implements TelemetryObservation { }
    record AwaitInteractionCreated(String stepId, String transport, Instant occurredAt) implements TelemetryObservation { }
    record AwaitProviderDispatched(String stepId, String transport, Instant occurredAt) implements TelemetryObservation { }
    record AwaitCompletionAdmitted(String stepId, String transport, Instant occurredAt) implements TelemetryObservation { }
    record AwaitLiveHandoff(String stepId, String transport, Instant occurredAt) implements TelemetryObservation { }
    record AwaitScalarContinuationStarted(String stepId, String transport, Instant occurredAt) implements TelemetryObservation { }
    record TerminalPublicationCompleted(String target, String provider, Instant occurredAt) implements TelemetryObservation { }
    record RetryScheduled(String stepClass, Instant occurredAt) implements TelemetryObservation { }
}
