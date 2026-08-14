/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    static final List<OperatorPanel> CSV_PAYMENTS_OPERATOR_PANELS = List.of(
        new OperatorPanel("Per-step throughput", List.of("tpf_step_duration_milliseconds_count")),
        new OperatorPanel("Per-step latency and success", List.of("tpf_step_duration_milliseconds_sum", "tpf_step_errors_total")),
        new OperatorPanel("Per-step in-flight and configured concurrency", List.of("tpf_step_inflight_items", "tpf_pipeline_max_concurrency_items")),
        new OperatorPanel("Step buffers and transition-worker pressure", List.of("tpf_step_buffer_queued_items", "tpf_orchestrator_transition_active")),
        new OperatorPanel("Await completion latency — p50, p90, p99", List.of("tpf_await_completion_latency_milliseconds_bucket")),
        new OperatorPanel("Await gate, routes, and terminal outcomes", List.of("tpf_await_admission_pending_reservations", "tpf_await_live_handoff_handoffs_total")),
        new OperatorPanel("CSV input and terminal publication", List.of("tpf_item_consumed_items_total", "tpf_object_publish_published_objects_total")),
        new OperatorPanel("Publication latency and output completeness", List.of("tpf_object_publish_write_duration_milliseconds_bucket")),
        new OperatorPanel("CSV operator SLO defaults", List.of("tpf_slo_item_success_good_items_total")),
        new OperatorPanel("JVM heap and GC pause", List.of("jvm_memory_used_bytes", "jvm_gc_pause_milliseconds_sum")));

    record OperatorPanel(String title, List<String> requiredMetricNames) { }

    private ObservabilityObligations() {
    }
}
