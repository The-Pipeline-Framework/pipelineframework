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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
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

    private final Tracer tracer;
    private final PipelineReplayExporter exporter;
    private final PipelineReplayTopology topology;
    private final LongCounter transitionCounter;
    private final DoubleHistogram transitionLatency;
    private final Map<String, PipelineReplayTopology.Step> stepsByRuntimeClass;
    private final Map<String, PipelineReplayTopology.Transition> inboundByRuntimeClass;
    private final Map<String, PipelineReplayTopology.Transition> outboundByRuntimeClass;
    private final ConcurrentMap<String, ConcurrentLinkedDeque<StepExecutionScope>> activeScopesByStep;

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
        this.activeScopesByStep = new ConcurrentHashMap<>();
    }

    boolean enabled() {
        return topology != null;
    }

    void runStarted(PipelineTelemetry.RunContext runContext) {
        if (!enabled() || runContext == null || runContext.replayState() == null) {
            return;
        }
        exporter.runStarted(topology.pipeline(), runContext.startedAt(), runContext.runParameters(), topology);
    }

    void runCompleted(PipelineTelemetry.RunContext runContext, long durationMs) {
        if (!enabled() || runContext == null || runContext.replayState() == null) {
            return;
        }
        exporter.runCompleted(topology.pipeline(), runContext.startedAt(), durationMs, topology);
    }

    void runFailed(PipelineTelemetry.RunContext runContext, long durationMs, Throwable failure) {
        if (!enabled() || runContext == null || runContext.replayState() == null) {
            return;
        }
        exporter.runFailed(topology.pipeline(), runContext.startedAt(), durationMs, topology, failure);
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
        attachInput(scope, inputItem);
        startIfNecessary(scope);
    }

    void recordOutput(StepExecutionScope scope, Object outputItem) {
        if (!enabled() || scope == null || outputItem == null || scope.runContext().replayState() == null) {
            return;
        }
        startIfNecessary(scope);
        ItemLineage outputLineage = createOutputLineage(scope, outputItem);
        if (outputLineage == null) {
            return;
        }
        PipelineReplayTopology.Transition outbound = scope.outbound();
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
        exporter.emit(event);
        addSpanEvent(scope.span(), "tpf.step.emit", Attributes.builder()
            .put(PIPELINE, topology.pipeline())
            .put(SOURCE_STEP, scope.descriptor().step())
            .put(TARGET_STEP, outbound == null ? "" : outbound.to())
            .put(CARDINALITY, scope.descriptor().cardinality())
            .build());
    }

    void recordRetry(String runtimeStepClass, Throwable failure) {
        if (!enabled() || runtimeStepClass == null) {
            return;
        }
        ConcurrentLinkedDeque<StepExecutionScope> scopes = activeScopesByStep.get(runtimeStepClass);
        if (scopes == null) {
            return;
        }
        StepExecutionScope scope = scopes.peekLast();
        if (scope == null || !scope.started()) {
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
        exporter.emit(event);
        addSpanEvent(scope.span(), "tpf.step.retry", Attributes.builder()
            .put(PIPELINE, topology.pipeline())
            .put(TARGET_STEP, scope.descriptor().step())
            .put(CARDINALITY, scope.descriptor().cardinality())
            .build());
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
            exporter.emit(event);
            addSpanEvent(scope.span(), failure == null ? "tpf.step.success" : "tpf.step.error",
                Attributes.builder()
                    .put(PIPELINE, topology.pipeline())
                    .put(TARGET_STEP, scope.descriptor().step())
                    .put(CARDINALITY, scope.descriptor().cardinality())
                    .build());
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
        if (scope.primaryInput() == null) {
            scope.primaryInput(lineage);
        }
    }

    private void startIfNecessary(StepExecutionScope scope) {
        if (!enabled() || scope == null || scope.started()) {
            return;
        }
        Context parentContext = scope.runContext().context();
        ItemLineage parentLineage = scope.primaryInput();
        if (parentLineage != null && parentLineage.lastSpanContext() != null && parentLineage.lastSpanContext().isValid()) {
            parentContext = parentContext.with(Span.wrap(parentLineage.lastSpanContext()));
        }
        Span span = tracer.spanBuilder("tpf.step")
            .setParent(parentContext)
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute("tpf.step.class", scope.runtimeStepClass())
            .setAttribute("tpf.step", scope.descriptor().step())
            .setAttribute("tpf.service", scope.descriptor().service())
            .setAttribute("tpf.pipeline", topology.pipeline())
            .setAttribute("tpf.cardinality", scope.descriptor().cardinality())
            .startSpan();
        scope.span(span);
        SpanContext spanContext = span.getSpanContext();
        SpanContext parentSpanContext = Span.fromContext(parentContext).getSpanContext();
        scope.traceId(spanContext.getTraceId());
        scope.spanId(spanContext.getSpanId());
        scope.parentSpanId(parentSpanContext != null && parentSpanContext.isValid() ? parentSpanContext.getSpanId() : null);
        long startNanos = System.nanoTime();
        scope.startNanos(startNanos);
        scope.startSeconds(secondsSinceRunStart(scope.runContext(), startNanos));
        scope.started(true);
        activeScopesByStep
            .computeIfAbsent(scope.runtimeStepClass(), ignored -> new ConcurrentLinkedDeque<>())
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
        exporter.emit(event);
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
        ConcurrentLinkedDeque<StepExecutionScope> scopes = activeScopesByStep.get(scope.runtimeStepClass());
        if (scopes == null) {
            return;
        }
        scopes.remove(scope);
        if (scopes.isEmpty()) {
            activeScopesByStep.remove(scope.runtimeStepClass(), scopes);
        }
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
        return new PipelineReplayTopology.Step(runtimeStepClass, step, service, "one-to-one", -1, false, null, null);
    }

    private PipelineReplayTopology.Transition inbound(String runtimeStepClass) {
        return inboundByRuntimeClass.get(runtimeStepClass);
    }

    private PipelineReplayTopology.Transition outbound(String runtimeStepClass) {
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

    private Map<String, PipelineReplayTopology.Transition> indexOutbound(PipelineReplayTopology topology) {
        if (topology == null || topology.transitions() == null) {
            return Map.of();
        }
        LinkedHashMap<String, PipelineReplayTopology.Transition> outbound = new LinkedHashMap<>();
        for (PipelineReplayTopology.Transition transition : topology.transitions()) {
            if (transition != null && transition.fromRuntimeStepClass() != null) {
                outbound.put(transition.fromRuntimeStepClass(), transition);
            }
        }
        return Map.copyOf(outbound);
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
        if (!enabled() || scope == null || !scope.started()) {
            return;
        }
        PipelineReplayTopology.Step cacheStep = resolvePluginStep(scope.descriptor().step(), "cache");
        if (cacheStep == null) {
            return;
        }
        double nowSeconds = secondsSinceRunStart(scope.runContext(), System.nanoTime());
        exporter.emit(newEvent(
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

    void recordReject(String runtimeStepClass, String rejectScope, String errorType, String errorMessage) {
        if (!enabled() || runtimeStepClass == null) {
            return;
        }
        ConcurrentLinkedDeque<StepExecutionScope> scopes = activeScopesByStep.get(runtimeStepClass);
        if (scopes == null) {
            return;
        }
        StepExecutionScope scope = scopes.peekLast();
        if (scope == null || !scope.started()) {
            return;
        }
        PipelineReplayTopology.Step rejectStep = resolvePluginStep(scope.descriptor().step(), "reject");
        String rejectStepName = rejectStep == null ? "Rejects " + scope.descriptor().step() : rejectStep.step();
        String rejectService = rejectStep == null ? "RejectQueue" : rejectStep.service();
        double nowSeconds = secondsSinceRunStart(scope.runContext(), System.nanoTime());
        exporter.emit(newEvent(
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
        private final PipelineReplayTopology.Transition outbound;
        private final PipelineTelemetry.RunContext runContext;
        private final boolean perItemOperation;
        private final List<ItemLineage> inputLineages;
        private final AtomicLong outputSequence;
        private final AtomicInteger retryAttempt;
        private ItemLineage primaryInput;
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
            PipelineReplayTopology.Transition outbound,
            PipelineTelemetry.RunContext runContext,
            boolean perItemOperation
        ) {
            this.runtimeStepClass = runtimeStepClass;
            this.descriptor = descriptor;
            this.inbound = inbound;
            this.outbound = outbound;
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

        PipelineReplayTopology.Transition outbound() {
            return outbound;
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
