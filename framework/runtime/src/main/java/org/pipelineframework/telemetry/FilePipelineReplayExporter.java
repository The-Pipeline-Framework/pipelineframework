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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import io.quarkus.arc.Unremovable;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Built-in replay exporter that writes replay JSON to a configured file.
 */
@ApplicationScoped
@Unremovable
public class FilePipelineReplayExporter implements PipelineReplayExporter {

    private static final Logger LOG = Logger.getLogger(FilePipelineReplayExporter.class);
    private static final String REPLAY_FILE_PATH_KEY = "pipeline.telemetry.replay.file.path";

    private final Path configuredOutputFile;
    private final ConcurrentMap<String, RunState> runStates = new ConcurrentHashMap<>();

    public FilePipelineReplayExporter() {
        this(resolveConfiguredOutputFile());
    }

    public FilePipelineReplayExporter(Path configuredOutputFile) {
        this.configuredOutputFile = configuredOutputFile == null ? null : configuredOutputFile.toAbsolutePath().normalize();
    }

    @Override
    public void runStarted(
        String runId,
        String pipeline,
        Instant startedAt,
        PipelineReplayRunParameters runParameters,
        PipelineReplayTopology topology
    ) {
        if (runId == null || runId.isBlank()) {
            return;
        }
        runStates.put(
            runId,
            new RunState(
                resolveOutputFileForRun(pipeline, startedAt),
                pipeline,
                startedAt,
                topology,
                runParameters));
    }

    @Override
    public void emit(String runId, PipelineExecutionEvent event) {
        if (runId == null || runId.isBlank() || event == null) {
            return;
        }
        RunState runState = runStates.get(runId);
        if (runState != null) {
            runState.events().add(event);
        }
    }

    @Override
    public void runCompleted(
        String runId,
        String pipeline,
        Instant startedAt,
        long durationMs,
        PipelineReplayTopology topology) {
        finalizeRun(runId, pipeline, startedAt, durationMs, topology, "completed", null, null);
    }

    @Override
    public void runFailed(
        String runId,
        String pipeline,
        Instant startedAt,
        long durationMs,
        PipelineReplayTopology topology,
        Throwable failure) {
        finalizeRun(
            runId,
            pipeline,
            startedAt,
            durationMs,
            topology,
            "failed",
            failure == null ? null : failure.getClass().getName(),
            failure == null ? null : failure.getMessage());
    }

    @PreDestroy
    void flushOnShutdown() {
        for (Map.Entry<String, RunState> entry : runStates.entrySet()) {
            RunState runState = entry.getValue();
            if (runState.durationMs() != null) {
                writeDocument(runState);
            }
        }
    }

    private void finalizeRun(
        String runId,
        String pipeline,
        Instant startedAt,
        long durationMs,
        PipelineReplayTopology topology,
        String status,
        String failureType,
        String failureMessage
    ) {
        if (runId == null || runId.isBlank()) {
            return;
        }
        RunState runState = runStates.computeIfAbsent(
            runId,
            ignored -> new RunState(resolveOutputFileForRun(pipeline, startedAt), pipeline, startedAt, topology, null));
        runState.pipeline(pipeline);
        runState.startedAt(startedAt);
        runState.topology(topology);
        runState.durationMs(durationMs);
        runState.status(status);
        runState.failureType(failureType);
        runState.failureMessage(failureMessage);
        writeDocument(runState);
        runStates.remove(runId, runState);
    }

    private void writeDocument(RunState runState) {
        if (runState == null
            || runState.outputFile() == null
            || runState.pipeline() == null
            || runState.startedAt() == null
            || runState.topology() == null) {
            return;
        }
        try {
            Path outputFile = runState.outputFile();
            Path parent = outputFile.toAbsolutePath().normalize().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            PipelineReplayDocument document = new PipelineReplayDocument(
                runState.pipeline(),
                runState.startedAt(),
                runState.durationMs(),
                runState.status() == null ? "completed" : runState.status(),
                runState.failureType(),
                runState.failureMessage(),
                runState.runParameters(),
                PipelineReplayTopologyAugmenter.augment(runState.topology(), runState.events()),
                List.copyOf(runState.events()));
            String json = PipelineJson.mapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(document);
            Files.writeString(outputFile, json + System.lineSeparator(), StandardCharsets.UTF_8);
            LOG.infof("Wrote replay JSON with %d events to %s.", runState.events().size(), outputFile);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to write replay JSON to %s.", runState.outputFile());
        }
    }

    private static Path resolveConfiguredOutputFile() {
        String configured = ConfigProvider.getConfig().getOptionalValue(REPLAY_FILE_PATH_KEY, String.class).orElse("").trim();
        if (configured.isBlank()) {
            return null;
        }
        return Path.of(configured);
    }

    private Path resolveOutputFileForRun(String pipeline, Instant startedAt) {
        if (configuredOutputFile == null) {
            return null;
        }
        if (!isDirectoryMode(configuredOutputFile)) {
            return configuredOutputFile;
        }
        String sanitizedPipeline = sanitizeFileToken(pipeline == null ? "pipeline" : pipeline);
        String startedAtToken = startedAt == null ? "unknown" : String.valueOf(startedAt.toEpochMilli());
        return configuredOutputFile.resolve(sanitizedPipeline + "-" + startedAtToken + "-" + UUID.randomUUID() + ".json");
    }

    private static boolean isDirectoryMode(Path path) {
        Path absolute = path.toAbsolutePath().normalize();
        if (Files.exists(absolute)) {
            return Files.isDirectory(absolute);
        }
        String fileName = absolute.getFileName() == null ? "" : absolute.getFileName().toString().toLowerCase(Locale.ROOT);
        return !fileName.endsWith(".json");
    }

    private static String sanitizeFileToken(String raw) {
        return raw.replaceAll("[^A-Za-z0-9._-]+", "-");
    }

    private static final class RunState {
        private final Path outputFile;
        private final List<PipelineExecutionEvent> events = java.util.Collections.synchronizedList(new ArrayList<>());
        private volatile String pipeline;
        private volatile Instant startedAt;
        private volatile PipelineReplayTopology topology;
        private volatile PipelineReplayRunParameters runParameters;
        private volatile Long durationMs;
        private volatile String status = "running";
        private volatile String failureType;
        private volatile String failureMessage;

        private RunState(
            Path outputFile,
            String pipeline,
            Instant startedAt,
            PipelineReplayTopology topology,
            PipelineReplayRunParameters runParameters
        ) {
            this.outputFile = outputFile;
            this.pipeline = pipeline;
            this.startedAt = startedAt;
            this.topology = topology;
            this.runParameters = runParameters;
        }

        Path outputFile() { return outputFile; }
        List<PipelineExecutionEvent> events() { return events; }
        String pipeline() { return pipeline; }
        void pipeline(String pipeline) { this.pipeline = pipeline; }
        Instant startedAt() { return startedAt; }
        void startedAt(Instant startedAt) { this.startedAt = startedAt; }
        PipelineReplayTopology topology() { return topology; }
        void topology(PipelineReplayTopology topology) { this.topology = topology; }
        PipelineReplayRunParameters runParameters() { return runParameters; }
        Long durationMs() { return durationMs; }
        void durationMs(Long durationMs) { this.durationMs = durationMs; }
        String status() { return status; }
        void status(String status) { this.status = status; }
        String failureType() { return failureType; }
        void failureType(String failureType) { this.failureType = failureType; }
        String failureMessage() { return failureMessage; }
        void failureMessage(String failureMessage) { this.failureMessage = failureMessage; }
    }
}
