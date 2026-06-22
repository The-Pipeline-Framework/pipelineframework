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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionReplayTrackerTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(sdk);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.shutdown();
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void mergeReplayScopeUsesCanonicalPrimaryInputRegardlessOfAttachOrder() {
        CollectingExporter exporter = new CollectingExporter();
        ExecutionReplayTracker tracker = new ExecutionReplayTracker(
            GlobalOpenTelemetry.getTracer("replay-test"),
            exporter,
            topology(),
            null,
            null);
        PipelineTelemetry.RunContext runContext = new PipelineTelemetry.RunContext(
            "run-1",
            Context.current(),
            null,
            System.nanoTime(),
            Instant.now(),
            Attributes.empty(),
            true,
            new AtomicLong(),
            new AtomicLong(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            null,
            new ExecutionReplayTracker.RunReplayState(),
            new AtomicBoolean());

        Object inputA = new Object();
        Object outputA = new Object();
        var sourceAScope = tracker.beginStep("source-a", runContext, true, inputA);
        tracker.recordOutput(sourceAScope, outputA);
        tracker.completeSuccess(sourceAScope);

        Object inputB = new Object();
        Object outputB = new Object();
        var sourceBScope = tracker.beginStep("source-b", runContext, true, inputB);
        tracker.recordOutput(sourceBScope, outputB);
        tracker.completeSuccess(sourceBScope);

        var reverseScope = tracker.beginPendingStep("merge-step", runContext, false);
        tracker.recordInput(reverseScope, outputB);
        assertFalse(reverseScope.started());
        tracker.recordInput(reverseScope, outputA);
        assertFalse(reverseScope.started());
        tracker.recordOutput(reverseScope, new Object());
        tracker.completeSuccess(reverseScope);

        var forwardScope = tracker.beginPendingStep("merge-step", runContext, false);
        tracker.recordInput(forwardScope, outputA);
        assertFalse(forwardScope.started());
        tracker.recordInput(forwardScope, outputB);
        assertFalse(forwardScope.started());
        tracker.recordOutput(forwardScope, new Object());
        tracker.completeSuccess(forwardScope);

        assertEquals(forwardScope.eventItemId(), reverseScope.eventItemId());
        assertEquals(forwardScope.traceId(), reverseScope.traceId());
        assertEquals(forwardScope.parentSpanId(), reverseScope.parentSpanId());
        assertEquals(forwardScope.parentItemIds(), reverseScope.parentItemIds());

        PipelineExecutionEvent reverseStart = exporter.find("start", reverseScope.spanId());
        PipelineExecutionEvent forwardStart = exporter.find("start", forwardScope.spanId());
        assertEquals(forwardStart.itemId(), reverseStart.itemId());
        assertEquals(forwardStart.traceId(), reverseStart.traceId());
        assertEquals(forwardStart.parentSpanId(), reverseStart.parentSpanId());
        assertEquals(forwardStart.parentItemIds(), reverseStart.parentItemIds());
        assertTrue(forwardStart.startTime() >= 0d);
    }

    @Test
    void connectorLifecycleEventsAreExportedAsReplayControlEvents() {
        CollectingExporter exporter = new CollectingExporter();
        ExecutionReplayTracker tracker = new ExecutionReplayTracker(
            GlobalOpenTelemetry.getTracer("replay-test"),
            exporter,
            topology(),
            null,
            null);

        tracker.recordConnectorEvent(
            "ObjectIngest",
            "ObjectIngestConnector",
            "object_ingest_submitted",
            "ObjectIngest",
            "SourceA",
            java.util.Map.of("connector", "object-ingest", "key", "payments.csv"),
            Instant.now());
        tracker.recordConnectorEvent(
            "ObjectPublish",
            "ObjectPublishConnector",
            "object_publish_published",
            "Merge",
            "ObjectPublish",
            java.util.Map.of("connector", "object-publish", "key", "payments.out"),
            Instant.now());

        PipelineExecutionEvent ingest = exporter.find("object_ingest_submitted", null);
        assertEquals("ObjectIngest", ingest.step());
        assertEquals("ObjectIngestConnector", ingest.service());
        assertEquals("object-ingest", ingest.attributes().get("connector"));
        assertEquals("payments.csv", ingest.attributes().get("key"));

        PipelineExecutionEvent publish = exporter.find("object_publish_published", null);
        assertEquals("ObjectPublish", publish.step());
        assertEquals("ObjectPublishConnector", publish.service());
        assertEquals("object-publish", publish.attributes().get("connector"));
        assertEquals("payments.out", publish.attributes().get("key"));
    }

    private PipelineReplayTopology topology() {
        return new PipelineReplayTopology(
            "csv-payments",
            List.of(
                new PipelineReplayTopology.Step("object-ingest", "ObjectIngest", "ObjectIngestConnector",
                    "one-to-one", -1, false, null, null, "object-ingest", null),
                new PipelineReplayTopology.Step("source-a", "SourceA", "SourceAService", "one-to-one", 0, false, null, null),
                new PipelineReplayTopology.Step("source-b", "SourceB", "SourceBService", "one-to-one", 1, false, null, null),
                new PipelineReplayTopology.Step("merge-step", "Merge", "MergeService", "many-to-one", 2, false, null, null),
                new PipelineReplayTopology.Step("object-publish", "ObjectPublish", "ObjectPublishConnector",
                    "one-to-one", 3, false, null, null, "object-publish", null)
            ),
            List.of(
                new PipelineReplayTopology.Transition("ingest->a", "object-ingest", "source-a", "ObjectIngest", "SourceA",
                    "ObjectIngestConnector", "SourceAService", "one-to-one"),
                new PipelineReplayTopology.Transition("a->merge", "source-a", "merge-step", "SourceA", "Merge",
                    "SourceAService", "MergeService", "one-to-one"),
                new PipelineReplayTopology.Transition("b->merge", "source-b", "merge-step", "SourceB", "Merge",
                    "SourceBService", "MergeService", "one-to-one"),
                new PipelineReplayTopology.Transition("merge->publish", "merge-step", "object-publish", "Merge", "ObjectPublish",
                    "MergeService", "ObjectPublishConnector", "one-to-one")
            ));
    }

    private static final class CollectingExporter implements PipelineReplayExporter {
        private final List<PipelineExecutionEvent> events = new ArrayList<>();

        @Override
        public void emit(String runId, PipelineExecutionEvent event) {
            events.add(event);
        }

        @Override
        public void emitControlEvent(
            String pipeline,
            Instant occurredAt,
            PipelineReplayTopology topology,
            PipelineExecutionEvent event
        ) {
            events.add(event);
        }

        private PipelineExecutionEvent find(String eventName, String spanId) {
            return events.stream()
                .filter(event -> eventName.equals(event.event())
                    && (spanId == null || spanId.equals(event.spanId())))
                .findFirst()
                .orElseThrow();
        }
    }
}
