/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

final class ExecutionReplayTracker {

    private static final AttributeKey<String> PIPELINE = AttributeKey.stringKey("tpf.pipeline");
    private static final AttributeKey<String> SOURCE_STEP = AttributeKey.stringKey("tpf.source.step");
    private static final AttributeKey<String> TARGET_STEP = AttributeKey.stringKey("tpf.target.step");
    private static final AttributeKey<String> SOURCE_SERVICE = AttributeKey.stringKey("tpf.source.service");
    private static final AttributeKey<String> TARGET_SERVICE = AttributeKey.stringKey("tpf.target.service");
    private static final AttributeKey<String> CARDINALITY = AttributeKey.stringKey("tpf.cardinality");
    private static final String AWAIT_UNIT_ID = "tpf.await.unit_id";
    private static final String AWAIT_EXECUTION_ID = "tpf.await.execution_id";
    private static final String AWAIT_STEP_ID = "tpf.await.step_id";
    private static final String AWAIT_STEP_INDEX = "tpf.await.step_index";
    private static final String AWAIT_STATUS = "tpf.await.status";
    private static final String AWAIT_INTERACTION_ID = "tpf.await.interaction_id";
    private static final String AWAIT_CORRELATION_ID = "tpf.await.correlation_id";
    private static final String AWAIT_TRANSPORT = "tpf.await.transport";
    private static final String AWAIT_ITEM_INDEX = "tpf.await.item_index";
    private static final String AWAIT_EXPECTED_ITEM_COUNT = "tpf.await.expected_item_count";
    private static final String AWAIT_COMPLETED_ITEM_COUNT = "tpf.await.completed_item_count";
    private static final String AWAIT_DISPATCH_COMPLETE = "tpf.await.dispatch_complete";

    private final Tracer tracer;
    private final PipelineReplayExporter exporter;
    private final PipelineReplayTopology topology;
    private final LongCounter transitionCounter;
    private final DoubleHistogram transitionLatency;
    private final Map<String, PipelineReplayTopology.Step> stepsByRuntimeClass;
    private final Map<String, PipelineReplayTopology.Transition> inboundByRuntimeClass;
    private final Map<String, List<PipelineReplayTopology.Transition>> outboundByRuntimeClass;
    private final ConcurrentMap<String, StepExecutionScope> activeScopesBySpanId;
    private final ConcurrentMap<String, java.util.Set<StepExecutionScope>> activeScopesByStepClass;

    ExecutionReplayTracker(
        Tracer tracer,
        PipelineReplayExporter exporter,
        PipelineReplayTopology topology,
        LongCounter transitionCounter,
        DoubleHistogram transitionLatency
    ) {
        this.tracer = tracer;
        this.exporter = exporter;
        this.topology = topology;
        this.transitionCounter = transitionCounter;
        this.transitionLatency = transitionLatency;
        this.stepsByRuntimeClass = topology == null ? Map.of() : topology.stepsByRuntimeClass();
        this.inboundByRuntimeClass = indexInbound(topology);
        this.outboundByRuntimeClass = indexOutbound(topology);
        this.activeScopesBySpanId = new ConcurrentHashMap<>();
        this.activeScopesByStepClass = new ConcurrentHashMap<>();
    }

    boolean enabled() {
        return topology != null;
    }

    void runStarted(PipelineTelemetry.RunContext runContext) {
        if (!enabled() || runContext == null || runContext.replayState() == null) {
            return;
        }
        exporter.runStarted(runContext.runId(), topology.pipeline(), runContext.startedAt(), runContext.runParameters(), topology);
    }

    void runCompleted(PipelineTelemetry.RunContext runContext, long durationMs) {
        if (!enabled() || runContext == null || runContext.replayState() == null) {
            return;
        }
        exporter.runCompleted(runContext.runId(), topology.pipeline(), runContext.startedAt(), durationMs, topology);
    }

    void runFailed(PipelineTelemetry.RunContext runContext, long durationMs, Throwable failure) {
        if (!enabled() || runContext == null || runContext.replayState() == null) {
            return;
        }
        exporter.runFailed(runContext.runId(), topology.pipeline(), runContext.startedAt(), durationMs, topology, failure);
    }

    void recordAwaitLifecycle(AwaitReplayLifecycleEvent lifecycleEvent, Instant occurredAt) {
        if (!enabled() || lifecycleEvent == null) {
            return;
        }
        PipelineReplayTopology.Step step = resolveAwaitStep(lifecycleEvent.stepId(), lifecycleEvent.stepIndex());
        String stepName = step == null ? lifecycleEvent.stepId() : step.step();
        if (stepName == null || stepName.isBlank()) {
            stepName = "Await";
        }
        String to = AwaitReplayLifecycleEvent.RESUME_RELEASED.equals(lifecycleEvent.eventName())
            ? resumeTargetFor(stepName)
            : stepName;
        PipelineExecutionEvent event = new PipelineExecutionEvent(
            null,
            null,
            null,
            lifecycleEvent.unitId(),
            topology.pipeline(),
            stepName,
            step == null ? stepName : step.service(),
            lifecycleEvent.eventName(),
            0d,
            0d,
            0L,
            stepName,
            to,
            step == null ? null : step.cardinality(),
            List.of(),
            null,
            null,
            null,
            null,
            awaitAttributes(lifecycleEvent, stepName));
        exporter.emitControlEvent(topology.pipeline(), occurredAt, topology, event);
        addAwaitSpanEvent(lifecycleEvent);
    }

    void recordConnectorEvent(
        String connectorStep,
        String service,
        String eventName,
        String from,
        String to,
        Map<String, String> attributes,
        Instant occurredAt
    ) {
        if (!enabled() || connectorStep == null || connectorStep.isBlank() || eventName == null || eventName.isBlank()) {
            return;
        }
        PipelineReplayTopology.Step step = topology.steps() == null
            ? null
            : topology.steps().stream()
                .filter(candidate -> connectorStep.equals(candidate.step()))
                .findFirst()
                .orElse(null);
        String resolvedService = service == null || service.isBlank()
            ? (step == null ? connectorStep : step.service())
            : service;
        PipelineExecutionEvent event = new PipelineExecutionEvent(
            null,
            null,
            null,
            null,
            topology.pipeline(),
            connectorStep,
            resolvedService,
            eventName,
            0d,
            0d,
            0L,
            from,
            to,
            step == null ? "one-to-one" : step.cardinality(),
            List.of(),
            null,
            null,
            null,
            null,
            attributes == null ? Map.of() : Map.copyOf(attributes));
        exporter.emitControlEvent(topology.pipeline(), occurredAt, topology, event);
    }

    void recordSkip(
        String runtimeStepClass,
        PipelineTelemetry.RunContext runContext,
        Object inputItem,
        String currentType,
        List<String> acceptedTypes
    ) {
        if (!enabled() || runtimeStepClass == null || runContext == null || runContext.replayState() == null) {
            return;
        }
        PipelineReplayTopology.Step descriptor = descriptor(runtimeStepClass);
        if (descriptor == null) {
            return;
        }
        PipelineReplayTopology.Transition inbound = inbound(runtimeStepClass);
        ItemLineage lineage = inputItem == null ? null : lookupOrCreateLineage(runContext, inputItem);
        double nowSeconds = secondsSinceRunStart(runContext, System.nanoTime());
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("reason", "not_applicable");
        if (currentType != null && !currentType.isBlank()) {
            attributes.put("currentType", currentType);
        }
        if (acceptedTypes != null && !acceptedTypes.isEmpty()) {
            attributes.put("acceptedTypes", String.join(",", acceptedTypes));
        }
        PipelineExecutionEvent event = new PipelineExecutionEvent(
            lineage == null ? null : lineage.traceId(),
            null,
            null,
            lineage == null ? null : lineage.itemId(),
            topology.pipeline(),
            descriptor.step(),
            descriptor.service(),
            "skip",
            nowSeconds,
            nowSeconds,
            0L,
            inbound == null ? null : inbound.from(),
            descriptor.step(),
            descriptor.cardinality(),
            lineage == null ? List.of() : lineage.parentItemIds(),
            runContext.replayState().eventSequence().incrementAndGet(),
            null,
            null,
            null,
            Map.copyOf(attributes));
        exporter.emit(runContext.runId(), event);
    }

    StepExecutionScope beginStep(
        String runtimeStepClass,
        PipelineTelemetry.RunContext runContext,
        boolean perItemOperation,
        Object inputItem
    ) {
        StepExecutionScope scope = new StepExecutionScope(runtimeStepClass, descriptor(runtimeStepClass), inbound(runtimeStepClass),
            outbound(runtimeStepClass), runContext, perItemOperation);
        attachInput(scope, inputItem);
        startIfNecessary(scope);
        return scope;
    }

    StepExecutionScope beginPendingStep(
        String runtimeStepClass,
        PipelineTelemetry.RunContext runContext,
        boolean perItemOperation
    ) {
        return new StepExecutionScope(runtimeStepClass, descriptor(runtimeStepClass), inbound(runtimeStepClass),
            outbound(runtimeStepClass), runContext, perItemOperation);
    }

    void recordInput(StepExecutionScope scope, Object inputItem) {
        if (!enabled() || scope == null || inputItem == null || scope.runContext().replayState() == null) {
            return;
        }
        synchronized (scope) {
            attachInput(scope, inputItem);
            if (shouldStartOnInput(scope)) {
                startIfNecessary(scope);
            }
        }
    }

    void recordOutput(StepExecutionScope scope, Object outputItem) {
        if (!enabled() || scope == null || outputItem == null || scope.runContext().replayState() == null) {
            return;
        }
        synchronized (scope) {
            startIfNecessary(scope);
            ItemLineage outputLineage = createOutputLineage(scope, outputItem);
            if (outputLineage == null) {
                return;
            }
            PipelineReplayTopology.Transition outbound = selectDataFlowTransition(scope);
            double nowSeconds = secondsSinceRunStart(scope.runContext(), System.nanoTime());
            if (outbound != null) {
                long latencyMs = Math.max(0L, Math.round((System.nanoTime() - scope.startNanos()) / 1_000_000d));
                recordTransitionMetrics(outbound, latencyMs);
            }
            PipelineExecutionEvent event = newEvent(
                scope,
                outputLineage.itemId(),
                scope.descriptor().step(),
                scope.descriptor().service(),
                "emit",
                nowSeconds,
                nowSeconds,
                0L,
                scope.descriptor().step(),
                outbound == null ? null : outbound.to(),
                scope.descriptor().cardinality(),
                outputLineage.parentItemIds(),
                scope.retryAttempt().get() == 0 ? null : scope.retryAttempt().get(),
                null,
                null,
                Map.of());
            exporter.emit(scope.runContext().runId(), event);
            addSpanEvent(scope.span(), "tpf.step.emit", Attributes.builder()
                .put(PIPELINE, topology.pipeline())
                .put(SOURCE_STEP, scope.descriptor().step())
                .put(TARGET_STEP, outbound == null ? "" : outbound.to())
                .put(CARDINALITY, scope.descriptor().cardinality())
                .build());
        }
    }

    void recordRetry(String runtimeStepClass, String spanId, Throwable failure) {
        if (!enabled() || runtimeStepClass == null) {
            return;
        }
        StepExecutionScope scope = resolveActiveScope(runtimeStepClass, spanId);
        if (scope == null) {
            return;
        }
        synchronized (scope) {
            startIfNecessary(scope);
            if (!scope.started()) {
                return;
            }
            int attempt = scope.retryAttempt().incrementAndGet();
            double nowSeconds = secondsSinceRunStart(scope.runContext(), System.nanoTime());
            PipelineExecutionEvent event = newEvent(
                scope,
                scope.eventItemId(),
                scope.descriptor().step(),
                scope.descriptor().service(),
                "retry",
                nowSeconds,
                nowSeconds,
                0L,
                scope.inbound() == null ? null : scope.inbound().from(),
                scope.descriptor().step(),
                scope.descriptor().cardinality(),
                scope.parentItemIds(),
                attempt,
                failure == null ? null : failure.getClass().getName(),
                failure == null ? null : failure.getMessage(),
                Map.of());
            exporter.emit(scope.runContext().runId(), event);
            addSpanEvent(scope.span(), "tpf.step.retry", Attributes.builder()
                .put(PIPELINE, topology.pipeline())
                .put(TARGET_STEP, scope.descriptor().step())
                .put(CARDINALITY, scope.descriptor().cardinality())
                .build());
        }
    }

    void completeSuccess(StepExecutionScope scope) {
        complete(scope, null);
    }

    void completeFailure(StepExecutionScope scope, Throwable failure) {
        complete(scope, failure);
    }

    private void complete(StepExecutionScope scope, Throwable failure) {
        if (scope == null) {
            return;
        }
        synchronized (scope) {
            if (enabled() && scope.runContext().replayState() != null) {
                startIfNecessary(scope);
                long endNanos = System.nanoTime();
                long durationMs = Math.max(0L, Math.round((endNanos - scope.startNanos()) / 1_000_000d));
                String eventName = failure == null ? "success" : "error";
                PipelineExecutionEvent event = newEvent(
                    scope,
                    scope.eventItemId(),
                    scope.descriptor().step(),
                    scope.descriptor().service(),
                    eventName,
                    scope.startSeconds(),
                    secondsSinceRunStart(scope.runContext(), endNanos),
                    durationMs,
                    scope.inbound() == null ? null : scope.inbound().from(),
                    scope.descriptor().step(),
                    scope.descriptor().cardinality(),
                    scope.parentItemIds(),
                    scope.retryAttempt().get() == 0 ? null : scope.retryAttempt().get(),
                    failure == null ? null : failure.getClass().getName(),
                    failure == null ? null : failure.getMessage(),
                    Map.of());
                exporter.emit(scope.runContext().runId(), event);
                addSpanEvent(scope.span(), failure == null ? "tpf.step.success" : "tpf.step.error",
                    Attributes.builder()
                        .put(PIPELINE, topology.pipeline())
                        .put(TARGET_STEP, scope.descriptor().step())
                        .put(CARDINALITY, scope.descriptor().cardinality())
                        .build());
            }
        }
        removeScope(scope);
        endSpan(scope.span(), failure);
    }

    private void attachInput(StepExecutionScope scope, Object inputItem) {
        RunReplayState replayState = scope.runContext().replayState();
        if (replayState == null || inputItem == null) {
            return;
        }
        ItemLineage lineage = lookupOrCreateLineage(scope.runContext(), inputItem);
        scope.inputLineages().add(lineage);
        if (scope.firstInputObservedAt() == null) {
            scope.firstInputObservedAt(Instant.now());
            scope.firstInputObservedNanos(System.nanoTime());
        }
    }

    private void startIfNecessary(StepExecutionScope scope) {
        if (!enabled() || scope == null || scope.started()) {
            return;
        }
        ItemLineage parentLineage = resolvePrimaryInput(scope);
        scope.primaryInput(parentLineage);
        Context parentContext = scope.runContext().context();
        if (parentLineage != null && parentLineage.lastSpanContext() != null && parentLineage.lastSpanContext().isValid()) {
            parentContext = parentContext.with(Span.wrap(parentLineage.lastSpanContext()));
        }
        var spanBuilder = tracer.spanBuilder("tpf.step")
            .setParent(parentContext)
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute("tpf.step.class", scope.runtimeStepClass())
            .setAttribute("tpf.step", scope.descriptor().step())
            .setAttribute("tpf.service", scope.descriptor().service())
            .setAttribute("tpf.pipeline", topology.pipeline())
            .setAttribute("tpf.cardinality", scope.descriptor().cardinality());
        if (scope.firstInputObservedAt() != null) {
            spanBuilder.setStartTimestamp(scope.firstInputObservedAt());
        }
        Span span = spanBuilder.startSpan();
        scope.span(span);
        SpanContext spanContext = span.getSpanContext();
        SpanContext parentSpanContext = Span.fromContext(parentContext).getSpanContext();
        scope.traceId(spanContext.getTraceId());
        scope.spanId(spanContext.getSpanId());
        scope.parentSpanId(parentSpanContext != null && parentSpanContext.isValid() ? parentSpanContext.getSpanId() : null);
        long startNanos = scope.firstInputObservedNanos() > 0L ? scope.firstInputObservedNanos() : System.nanoTime();
        scope.startNanos(startNanos);
        scope.startSeconds(secondsSinceRunStart(scope.runContext(), startNanos));
        scope.started(true);
        activeScopesBySpanId.put(scope.spanId(), scope);
        activeScopesByStepClass
            .computeIfAbsent(scope.runtimeStepClass(), ignored -> ConcurrentHashMap.newKeySet())
            .add(scope);
        PipelineExecutionEvent event = newEvent(
            scope,
            scope.eventItemId(),
            scope.descriptor().step(),
            scope.descriptor().service(),
            "start",
            scope.startSeconds(),
            scope.startSeconds(),
            0L,
            scope.inbound() == null ? null : scope.inbound().from(),
            scope.descriptor().step(),
            scope.descriptor().cardinality(),
            scope.parentItemIds(),
            null,
            null,
            null,
            Map.of());
        exporter.emit(scope.runContext().runId(), event);
        addSpanEvent(span, "tpf.step.start", Attributes.builder()
            .put(PIPELINE, topology.pipeline())
            .put(TARGET_STEP, scope.descriptor().step())
            .put(CARDINALITY, scope.descriptor().cardinality())
            .build());
    }

    private ItemLineage createOutputLineage(StepExecutionScope scope, Object outputItem) {
        RunReplayState replayState = scope.runContext().replayState();
        if (replayState == null) {
            return null;
        }
        List<ItemLineage> inputs = orderedInputs(scope.inputLineages());
        String cardinality = scope.descriptor().cardinality();
        ItemLineage outputLineage;
        if ("one-to-one".equals(cardinality) && scope.primaryInput() != null) {
            outputLineage = scope.primaryInput().advance(scope.runtimeStepClass(), scope.span().getSpanContext(),
                scope.descriptor().step(), scope.descriptor().service(), List.of());
        } else if ("one-to-many".equals(cardinality) && scope.primaryInput() != null) {
            String itemId = deterministicId("fanout", scope.primaryInput().itemId(), scope.descriptor().step(),
                Long.toString(scope.outputSequence().incrementAndGet()));
            outputLineage = new ItemLineage(itemId, List.of(scope.primaryInput().itemId()), scope.traceId(),
                scope.runtimeStepClass(), scope.descriptor().step(), scope.descriptor().service(),
                scope.span().getSpanContext());
        } else if (("many-to-one".equals(cardinality) || "many-to-many".equals(cardinality)) && !inputs.isEmpty()) {
            List<String> parentIds = inputs.stream().map(ItemLineage::itemId).sorted().toList();
            String itemId = deterministicId("fanin", scope.descriptor().step(), String.join("|", parentIds));
            outputLineage = new ItemLineage(itemId, parentIds, scope.traceId(), scope.runtimeStepClass(),
                scope.descriptor().step(), scope.descriptor().service(), scope.span().getSpanContext());
        } else {
            String itemId = "item-%06d".formatted(replayState.itemSequence().incrementAndGet());
            outputLineage = new ItemLineage(itemId, List.of(), scope.traceId(), scope.runtimeStepClass(),
                scope.descriptor().step(), scope.descriptor().service(), scope.span().getSpanContext());
        }
        replayState.store(outputItem, outputLineage);
        return outputLineage;
    }

    private List<ItemLineage> orderedInputs(List<ItemLineage> inputs) {
        return inputs.stream()
            .filter(lineage -> lineage != null && lineage.itemId() != null)
            .sorted(Comparator.comparing(ItemLineage::itemId))
            .toList();
    }

    private ItemLineage resolvePrimaryInput(StepExecutionScope scope) {
        List<ItemLineage> inputs = orderedInputs(scope.inputLineages());
        return inputs.isEmpty() ? null : inputs.getFirst();
    }

    private boolean shouldStartOnInput(StepExecutionScope scope) {
        if (scope == null) {
            return false;
        }
        String cardinality = scope.descriptor().cardinality();
        return !"many-to-one".equals(cardinality) && !"many-to-many".equals(cardinality);
    }

    private ItemLineage lookupOrCreateLineage(PipelineTelemetry.RunContext runContext, Object item) {
        RunReplayState replayState = runContext.replayState();
        if (replayState == null) {
            return null;
        }
        ItemLineage existing = replayState.lookup(item);
        if (existing != null) {
            return existing;
        }
        String traceId = runContext.span() != null && runContext.span().getSpanContext().isValid()
            ? runContext.span().getSpanContext().getTraceId()
            : deterministicId("trace", runContext.startedAt().toString());
        ItemLineage created = new ItemLineage(
            "item-%06d".formatted(replayState.itemSequence().incrementAndGet()),
            List.of(),
            traceId,
            null,
            null,
            null,
            null);
        replayState.store(item, created);
        return created;
    }

    private void removeScope(StepExecutionScope scope) {
        if (scope.spanId() != null) {
            activeScopesBySpanId.remove(scope.spanId(), scope);
        }
        java.util.Set<StepExecutionScope> scopes = activeScopesByStepClass.get(scope.runtimeStepClass());
        if (scopes != null) {
            scopes.remove(scope);
            if (scopes.isEmpty()) {
                activeScopesByStepClass.remove(scope.runtimeStepClass(), scopes);
            }
        }
    }

    private PipelineReplayTopology.Transition selectDataFlowTransition(StepExecutionScope scope) {
        if (scope == null || scope.outbounds() == null || scope.outbounds().isEmpty()) {
            return null;
        }
        for (PipelineReplayTopology.Transition transition : scope.outbounds()) {
            if (transition == null) {
                continue;
            }
            PipelineReplayTopology.Step target = descriptor(transition.toRuntimeStepClass());
            if (!target.sideEffect()) {
                return transition;
            }
        }
        return scope.outbounds().getFirst();
    }

    private StepExecutionScope resolveActiveScope(String runtimeStepClass, String spanId) {
        if (spanId != null && !spanId.isBlank()) {
            StepExecutionScope bySpan = activeScopesBySpanId.get(spanId);
            if (bySpan != null) {
                return bySpan;
            }
        }
        if (runtimeStepClass == null) {
            return null;
        }
        java.util.Set<StepExecutionScope> scopes = activeScopesByStepClass.get(runtimeStepClass);
        if (scopes == null || scopes.size() != 1) {
            return null;
        }
        return scopes.iterator().next();
    }

    private void recordTransitionMetrics(PipelineReplayTopology.Transition transition, long latencyMs) {
        Attributes attributes = Attributes.builder()
            .put(PIPELINE, topology.pipeline())
            .put(SOURCE_STEP, transition.from())
            .put(TARGET_STEP, transition.to())
            .put(SOURCE_SERVICE, transition.fromService())
            .put(TARGET_SERVICE, transition.toService())
            .put(CARDINALITY, transition.cardinality())
            .build();
        if (transitionCounter != null) {
            transitionCounter.add(1, attributes);
        }
        if (transitionLatency != null) {
            transitionLatency.record(latencyMs, attributes);
        }
    }

    private void addSpanEvent(Span span, String name, Attributes attributes) {
        if (span != null && name != null) {
            span.addEvent(name, attributes == null ? Attributes.empty() : attributes);
        }
    }

    private void endSpan(Span span, Throwable failure) {
        if (span == null) {
            return;
        }
        if (failure != null) {
            span.recordException(failure);
            span.setStatus(StatusCode.ERROR, failure.getMessage());
        }
        span.end();
    }

    private PipelineReplayTopology.Step descriptor(String runtimeStepClass) {
        PipelineReplayTopology.Step resolved = stepsByRuntimeClass.get(runtimeStepClass);
        if (resolved != null) {
            return resolved;
        }
        String step = simpleName(runtimeStepClass)
            .replace("GrpcClientStep", "")
            .replace("RestClientStep", "")
            .replace("LocalClientStep", "")
            .replace("Service", "");
        String service = step.endsWith("Service") ? step : step + "Service";
        String renderRole = step.toLowerCase(Locale.ROOT).contains("await") ? "await" : "primary";
        return new PipelineReplayTopology.Step(runtimeStepClass, step, service, "one-to-one", -1, false, null, null, renderRole, null);
    }

    private PipelineReplayTopology.Step resolveAwaitStep(String stepId, Integer stepIndex) {
        if (topology == null || topology.steps() == null) {
            return null;
        }
        PipelineReplayTopology.Step firstAwaitStep = null;
        if (stepId != null && !stepId.isBlank()) {
            String normalizedStepId = normalizeAwaitStepId(stepId);
            for (PipelineReplayTopology.Step step : topology.steps()) {
                if (stepId.equals(step.step()) || normalizedStepId.equals(step.step())) {
                    return step;
                }
                if ("await".equals(step.renderRole()) && firstAwaitStep == null) {
                    firstAwaitStep = step;
                }
            }
        }
        if (stepIndex != null) {
            for (PipelineReplayTopology.Step step : topology.steps()) {
                if (step.index() == stepIndex && "await".equals(step.renderRole())) {
                    return step;
                }
                if ("await".equals(step.renderRole()) && firstAwaitStep == null) {
                    firstAwaitStep = step;
                }
            }
        }
        return firstAwaitStep;
    }

    private static String normalizeAwaitStepId(String stepId) {
        if (stepId == null || stepId.isBlank()) {
            return "";
        }
        String normalized = stepId;
        if (normalized.startsWith("Process") && normalized.length() > "Process".length()) {
            normalized = normalized.substring("Process".length());
        }
        if (normalized.endsWith("Service") && normalized.length() > "Service".length()) {
            normalized = normalized.substring(0, normalized.length() - "Service".length());
        }
        return normalized;
    }

    private String resumeTargetFor(String awaitStep) {
        if (awaitStep == null || topology == null || topology.transitions() == null) {
            return awaitStep;
        }
        return topology.transitions().stream()
            .filter(transition -> awaitStep.equals(transition.from()) && "primary".equals(transition.relationKind()))
            .map(PipelineReplayTopology.Transition::to)
            .findFirst()
            .orElse(awaitStep);
    }

    private Map<String, String> awaitAttributes(AwaitReplayLifecycleEvent lifecycleEvent, String resolvedStepName) {
        LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
        put(attributes, AWAIT_UNIT_ID, lifecycleEvent.unitId());
        put(attributes, AWAIT_EXECUTION_ID, lifecycleEvent.executionId());
        put(attributes, AWAIT_STEP_ID, lifecycleEvent.stepId() == null ? resolvedStepName : lifecycleEvent.stepId());
        put(attributes, AWAIT_STEP_INDEX, lifecycleEvent.stepIndex());
        put(attributes, AWAIT_STATUS, lifecycleEvent.status());
        put(attributes, AWAIT_INTERACTION_ID, lifecycleEvent.interactionId());
        put(attributes, AWAIT_CORRELATION_ID, lifecycleEvent.correlationId());
        put(attributes, AWAIT_TRANSPORT, lifecycleEvent.transport());
        put(attributes, AWAIT_ITEM_INDEX, lifecycleEvent.itemIndex());
        put(attributes, AWAIT_EXPECTED_ITEM_COUNT, lifecycleEvent.expectedItemCount());
        put(attributes, AWAIT_COMPLETED_ITEM_COUNT, lifecycleEvent.completedItemCount());
        put(attributes, AWAIT_DISPATCH_COMPLETE, lifecycleEvent.dispatchComplete());
        return Map.copyOf(attributes);
    }

    private void put(Map<String, String> attributes, String key, Object value) {
        if (value != null) {
            attributes.put(key, value.toString());
        }
    }

    private void addAwaitSpanEvent(AwaitReplayLifecycleEvent lifecycleEvent) {
        Span current = Span.current();
        if (current == null || !current.getSpanContext().isValid()) {
            return;
        }
        PipelineReplayTopology.Step step = resolveAwaitStep(lifecycleEvent.stepId(), lifecycleEvent.stepIndex());
        String stepName = step == null ? lifecycleEvent.stepId() : step.step();
        AttributesBuilder builder = Attributes.builder();
        awaitAttributes(lifecycleEvent, stepName).forEach((key, value) -> builder.put(AttributeKey.stringKey(key), value));
        current.addEvent("tpf.await." + lifecycleEvent.eventName(), builder.build());
    }

    private PipelineReplayTopology.Transition inbound(String runtimeStepClass) {
        return inboundByRuntimeClass.get(runtimeStepClass);
    }

    private List<PipelineReplayTopology.Transition> outbound(String runtimeStepClass) {
        return outboundByRuntimeClass.get(runtimeStepClass);
    }

    private Map<String, PipelineReplayTopology.Transition> indexInbound(PipelineReplayTopology topology) {
        if (topology == null || topology.transitions() == null) {
            return Map.of();
        }
        LinkedHashMap<String, PipelineReplayTopology.Transition> inbound = new LinkedHashMap<>();
        for (PipelineReplayTopology.Transition transition : topology.transitions()) {
            if (transition != null && transition.toRuntimeStepClass() != null) {
                inbound.put(transition.toRuntimeStepClass(), transition);
            }
        }
        return Map.copyOf(inbound);
    }

    private Map<String, List<PipelineReplayTopology.Transition>> indexOutbound(PipelineReplayTopology topology) {
        if (topology == null || topology.transitions() == null) {
            return Map.of();
        }
        LinkedHashMap<String, List<PipelineReplayTopology.Transition>> outbound = new LinkedHashMap<>();
        for (PipelineReplayTopology.Transition transition : topology.transitions()) {
            if (transition != null && transition.fromRuntimeStepClass() != null) {
                outbound.computeIfAbsent(transition.fromRuntimeStepClass(), ignored -> new ArrayList<>()).add(transition);
            }
        }
        LinkedHashMap<String, List<PipelineReplayTopology.Transition>> ordered = new LinkedHashMap<>();
        outbound.forEach((key, value) -> ordered.put(key, List.copyOf(value)));
        return java.util.Collections.unmodifiableMap(ordered);
    }

    private double secondsSinceRunStart(PipelineTelemetry.RunContext runContext, long nanos) {
        return (nanos - runContext.startNanos()) / 1_000_000_000d;
    }

    private String simpleName(String className) {
        if (className == null || className.isBlank()) {
            return "UnknownStep";
        }
        int lastDot = className.lastIndexOf('.');
        return lastDot >= 0 ? className.substring(lastDot + 1) : className;
    }

    private String deterministicId(String namespace, String... components) {
        StringBuilder builder = new StringBuilder();
        append(builder, namespace == null ? "" : namespace);
        if (components != null) {
            for (String component : components) {
                append(builder, component == null ? "" : component);
            }
        }
        return UUID.nameUUIDFromBytes(builder.toString().getBytes(StandardCharsets.UTF_8)).toString();
    }

    private void append(StringBuilder builder, String value) {
        builder.append('#').append(value.length()).append(':').append(value);
    }

    void recordCacheHit(StepExecutionScope scope) {
        if (!enabled() || scope == null) {
            return;
        }
        synchronized (scope) {
            startIfNecessary(scope);
            if (!scope.started()) {
                return;
            }
            PipelineReplayTopology.Step cacheStep = resolvePluginStep(scope.descriptor().step(), "cache");
            if (cacheStep == null) {
                return;
            }
            double nowSeconds = secondsSinceRunStart(scope.runContext(), System.nanoTime());
            exporter.emit(scope.runContext().runId(), newEvent(
                scope,
                scope.eventItemId(),
                scope.descriptor().step(),
                scope.descriptor().service(),
                "cache_hit",
                nowSeconds,
                nowSeconds,
                0L,
                cacheStep.step(),
                scope.descriptor().step(),
                scope.descriptor().cardinality(),
                scope.parentItemIds(),
                scope.retryAttempt().get() == 0 ? null : scope.retryAttempt().get(),
                null,
                null,
                Map.of("pluginKind", "cache")));
            addSpanEvent(scope.span(), "tpf.step.cache_hit", Attributes.builder()
                .put(PIPELINE, topology.pipeline())
                .put(SOURCE_STEP, cacheStep.step())
                .put(TARGET_STEP, scope.descriptor().step())
                .put(CARDINALITY, scope.descriptor().cardinality())
                .build());
        }
    }

    void recordReject(String runtimeStepClass, String spanId, String rejectScope, String errorType, String errorMessage) {
        if (!enabled() || runtimeStepClass == null) {
            return;
        }
        StepExecutionScope scope = resolveActiveScope(runtimeStepClass, spanId);
        if (scope == null) {
            return;
        }
        synchronized (scope) {
            startIfNecessary(scope);
            if (!scope.started()) {
                return;
            }
            PipelineReplayTopology.Step rejectStep = resolvePluginStep(scope.descriptor().step(), "reject");
            String rejectStepName = rejectStep == null ? "Rejects " + scope.descriptor().step() : rejectStep.step();
            String rejectService = rejectStep == null ? "RejectQueue" : rejectStep.service();
            double nowSeconds = secondsSinceRunStart(scope.runContext(), System.nanoTime());
            exporter.emit(scope.runContext().runId(), newEvent(
                scope,
                scope.eventItemId(),
                rejectStepName,
                rejectService,
                "reject",
                nowSeconds,
                nowSeconds,
                0L,
                scope.descriptor().step(),
                rejectStepName,
                scope.descriptor().cardinality(),
                scope.parentItemIds(),
                scope.retryAttempt().get() == 0 ? null : scope.retryAttempt().get(),
                errorType,
                errorMessage,
                rejectScope == null ? Map.of("pluginKind", "reject") : Map.of("pluginKind", "reject", "rejectScope", rejectScope)));
            addSpanEvent(scope.span(), "tpf.step.reject", Attributes.builder()
                .put(PIPELINE, topology.pipeline())
                .put(SOURCE_STEP, scope.descriptor().step())
                .put(TARGET_STEP, rejectStepName)
                .put(CARDINALITY, scope.descriptor().cardinality())
                .build());
        }
    }

    private PipelineExecutionEvent newEvent(
        StepExecutionScope scope,
        String itemId,
        String step,
        String service,
        String eventName,
        double startTime,
        double endTime,
        long durationMs,
        String from,
        String to,
        String cardinality,
        List<String> parentItemIds,
        Integer attempt,
        String errorType,
        String errorMessage,
        Map<String, String> attributes
    ) {
        return new PipelineExecutionEvent(
            scope.traceId(),
            scope.spanId(),
            scope.parentSpanId(),
            itemId,
            topology.pipeline(),
            step,
            service,
            eventName,
            startTime,
            endTime,
            durationMs,
            from,
            to,
            cardinality,
            parentItemIds,
            scope.runContext().replayState().eventSequence().incrementAndGet(),
            attempt,
            errorType,
            errorMessage,
            attributes == null || attributes.isEmpty() ? null : Map.copyOf(attributes));
    }

    private PipelineReplayTopology.Step resolvePluginStep(String parentStep, String pluginKind) {
        if (topology == null || topology.steps() == null || parentStep == null || pluginKind == null) {
            return null;
        }
        return topology.steps().stream()
            .filter(step -> step.sideEffect()
                && parentStep.equals(step.parentStep())
                && pluginKind.equals(step.pluginKind()))
            .findFirst()
            .orElse(null);
    }

    static final class RunReplayState {
        private final AtomicLong eventSequence = new AtomicLong();
        private final AtomicLong itemSequence = new AtomicLong();
        private final Map<Object, ItemLineage> itemLineages =
            Collections.synchronizedMap(new IdentityHashMap<>());

        AtomicLong eventSequence() {
            return eventSequence;
        }

        AtomicLong itemSequence() {
            return itemSequence;
        }

        ItemLineage lookup(Object item) {
            return itemLineages.get(item);
        }

        void store(Object item, ItemLineage lineage) {
            if (item != null && lineage != null) {
                itemLineages.put(item, lineage);
            }
        }
    }

    private record ItemLineage(
        String itemId,
        List<String> parentItemIds,
        String traceId,
        String lastStepClass,
        String lastStep,
        String lastService,
        SpanContext lastSpanContext
    ) {
        private ItemLineage advance(
            String runtimeStepClass,
            SpanContext spanContext,
            String step,
            String service,
            List<String> parentIds) {
            return new ItemLineage(itemId, parentIds == null ? List.of() : List.copyOf(parentIds), traceId,
                runtimeStepClass, step, service, spanContext);
        }
    }

    public static final class StepExecutionScope {
        private final String runtimeStepClass;
        private final PipelineReplayTopology.Step descriptor;
        private final PipelineReplayTopology.Transition inbound;
        private final List<PipelineReplayTopology.Transition> outbounds;
        private final PipelineTelemetry.RunContext runContext;
        private final boolean perItemOperation;
        private final List<ItemLineage> inputLineages;
        private final AtomicLong outputSequence;
        private final AtomicInteger retryAttempt;
        private ItemLineage primaryInput;
        private Instant firstInputObservedAt;
        private long firstInputObservedNanos;
        private Span span;
        private long startNanos;
        private double startSeconds;
        private String traceId;
        private String spanId;
        private String parentSpanId;
        private boolean started;

        private StepExecutionScope(
            String runtimeStepClass,
            PipelineReplayTopology.Step descriptor,
            PipelineReplayTopology.Transition inbound,
            List<PipelineReplayTopology.Transition> outbounds,
            PipelineTelemetry.RunContext runContext,
            boolean perItemOperation
        ) {
            this.runtimeStepClass = runtimeStepClass;
            this.descriptor = descriptor;
            this.inbound = inbound;
            this.outbounds = outbounds == null ? List.of() : List.copyOf(outbounds);
            this.runContext = runContext;
            this.perItemOperation = perItemOperation;
            this.inputLineages = new ArrayList<>();
            this.outputSequence = new AtomicLong(-1L);
            this.retryAttempt = new AtomicInteger();
        }

        String eventItemId() {
            return primaryInput == null ? null : primaryInput.itemId();
        }

        List<String> parentItemIds() {
            if (inputLineages.isEmpty()) {
                return List.of();
            }
            return inputLineages.stream()
                .map(ItemLineage::itemId)
                .filter(id -> id != null && !id.isBlank())
                .distinct()
                .sorted()
                .toList();
        }

        String runtimeStepClass() {
            return runtimeStepClass;
        }

        PipelineReplayTopology.Step descriptor() {
            return descriptor;
        }

        PipelineReplayTopology.Transition inbound() {
            return inbound;
        }

        List<PipelineReplayTopology.Transition> outbounds() {
            return outbounds;
        }

        PipelineTelemetry.RunContext runContext() {
            return runContext;
        }

        boolean perItemOperation() {
            return perItemOperation;
        }

        List<ItemLineage> inputLineages() {
            return inputLineages;
        }

        AtomicLong outputSequence() {
            return outputSequence;
        }

        AtomicInteger retryAttempt() {
            return retryAttempt;
        }

        ItemLineage primaryInput() {
            return primaryInput;
        }

        void primaryInput(ItemLineage primaryInput) {
            this.primaryInput = primaryInput;
        }

        Instant firstInputObservedAt() {
            return firstInputObservedAt;
        }

        void firstInputObservedAt(Instant firstInputObservedAt) {
            this.firstInputObservedAt = firstInputObservedAt;
        }

        long firstInputObservedNanos() {
            return firstInputObservedNanos;
        }

        void firstInputObservedNanos(long firstInputObservedNanos) {
            this.firstInputObservedNanos = firstInputObservedNanos;
        }

        Span span() {
            return span;
        }

        void span(Span span) {
            this.span = span;
        }

        long startNanos() {
            return startNanos;
        }

        void startNanos(long startNanos) {
            this.startNanos = startNanos;
        }

        double startSeconds() {
            return startSeconds;
        }

        void startSeconds(double startSeconds) {
            this.startSeconds = startSeconds;
        }

        String traceId() {
            return traceId;
        }

        void traceId(String traceId) {
            this.traceId = traceId;
        }

        String spanId() {
            return spanId;
        }

        void spanId(String spanId) {
            this.spanId = spanId;
        }

        String parentSpanId() {
            return parentSpanId;
        }

        void parentSpanId(String parentSpanId) {
            this.parentSpanId = parentSpanId;
        }

        boolean started() {
            return started;
        }

        void started(boolean started) {
            this.started = started;
        }
    }
}
