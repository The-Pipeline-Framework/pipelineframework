package org.pipelineframework.telemetry;

import java.util.LinkedHashMap;
import java.util.Map;

/** Pure, sink-specific attribute derivation. Metric attributes intentionally remain low-cardinality. */
public final class TelemetryAttributes {
    private TelemetryAttributes() {
    }

    public static Map<String, String> metricAttributes(TelemetryObservation observation) {
        return common(observation);
    }

    public static Map<String, String> spanAttributes(TelemetryObservation observation) {
        return common(observation);
    }

    public static Map<String, String> replayAttributes(TelemetryObservation observation) {
        return common(observation);
    }

    private static Map<String, String> common(TelemetryObservation observation) {
        Map<String, String> attributes = new LinkedHashMap<>();
        if (observation instanceof TelemetryObservation.RunStarted value) {
            attributes.put("tpf.pipeline", value.pipeline());
        } else if (observation instanceof TelemetryObservation.RunFinished value) {
            attributes.put("tpf.pipeline", value.pipeline());
            attributes.put("tpf.outcome", value.successful() ? "success" : "failure");
        } else if (observation instanceof TelemetryObservation.StepCompleted value) {
            attributes.put("tpf.step.class", value.stepClass());
            attributes.put("tpf.cardinality", value.cardinality());
            attributes.put("tpf.outcome", value.successful() ? "success" : "failure");
        } else if (observation instanceof TelemetryObservation.TransitionRejected value) {
            attributes.put("tpf.transition", value.transition());
            attributes.put("tpf.reason", value.reason());
        } else if (observation instanceof TelemetryObservation.TransitionDispatched) {
            attributes.put("tpf.transition.stage", "dispatched");
        } else if (observation instanceof TelemetryObservation.AwaitInteractionCreated value) {
            awaitAttributes(attributes, value.stepId(), value.transport());
        } else if (observation instanceof TelemetryObservation.AwaitProviderDispatched value) {
            awaitAttributes(attributes, value.stepId(), value.transport());
        } else if (observation instanceof TelemetryObservation.AwaitCompletionAdmitted value) {
            awaitAttributes(attributes, value.stepId(), value.transport());
        } else if (observation instanceof TelemetryObservation.AwaitLiveHandoff value) {
            awaitAttributes(attributes, value.stepId(), value.transport());
        } else if (observation instanceof TelemetryObservation.AwaitScalarContinuationStarted value) {
            awaitAttributes(attributes, value.stepId(), value.transport());
        } else if (observation instanceof TelemetryObservation.TerminalPublicationCompleted value) {
            attributes.put("tpf.object_publish.target", value.target());
            attributes.put("tpf.object_publish.provider", value.provider());
        } else if (observation instanceof TelemetryObservation.RetryScheduled value) {
            attributes.put("tpf.step.class", value.stepClass());
        }
        return Map.copyOf(attributes);
    }

    private static void awaitAttributes(Map<String, String> attributes, String stepId, String transport) {
        attributes.put("tpf.await.step_id", stepId);
        attributes.put("tpf.await.transport", transport);
    }
}
