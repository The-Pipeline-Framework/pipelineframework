package org.pipelineframework.telemetry;

import java.util.List;

/**
 * Test-only semantic contract shared by CSV runtime conformance and dashboard proof tests.
 * Each entry is a deliberate signal decision, not a catalogue of implementation methods.
 */
final class ObservabilityObligations {
    record Obligation(
        String transition,
        List<String> requiredMetricNames,
        List<String> requiredTraceSpanNames,
        String continuityRequirement
    ) { }

    static final List<Obligation> CSV_PAYMENTS_JOURNEY = List.of(
        new Obligation("PIPELINE_RUN", List.of("tpf_pipeline_run_count_total"), List.of("tpf.pipeline.run"), "pipeline root"),
        new Obligation("TRANSITION_DISPATCHED",
            List.of("tpf_orchestrator_transition_dispatched_transitions_total"),
            List.of("tpf.transition.dispatched"), "transition worker context"),
        new Obligation("AWAIT_INTERACTION_CREATED",
            List.of("tpf_await_interaction_created_interactions_total"),
            List.of("tpf.await.interaction.created"), "origin trace persisted with interaction"),
        new Obligation("AWAIT_PROVIDER_ADMITTED_AND_DISPATCHED",
            List.of("tpf_await_interaction_dispatched_interactions_total"),
            List.of("tpf.await.provider.dispatch", "tpf.await.provider.admitted"), "broker propagation"),
        new Obligation("AWAIT_COMPLETION_ADMITTED",
            List.of("tpf_await_completion_admitted_completions_total"),
            List.of("tpf.await.completion.admitted"), "parentage or durable SpanLink"),
        new Obligation("AWAIT_LIVE_HANDOFF",
            List.of("tpf_await_live_handoff_handoffs_total"),
            List.of("tpf.await.live.handoff"), "parentage or durable SpanLink"),
        new Obligation("SCALAR_CONTINUATION_STARTED",
            List.of("tpf_await_scalar_continuation_started_continuations_total"),
            List.of("tpf.await.scalar.continuation"), "parentage or durable SpanLink"),
        new Obligation("TERMINAL_PUBLICATION_COMPLETED",
            List.of("tpf_object_publish_published_objects_total"),
            List.of("tpf.terminal.publication.completed"), "continuation context"));

    private ObservabilityObligations() {
    }
}
