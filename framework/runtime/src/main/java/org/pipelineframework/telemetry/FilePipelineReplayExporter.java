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
import java.util.UUID;

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
    private final List<PipelineExecutionEvent> events = new ArrayList<>();
    private Path outputFile;
    private String pipeline;
    private Instant startedAt;
    private PipelineReplayTopology topology;
    private PipelineReplayRunParameters runParameters;
    private Long durationMs;
    private String status;
    private String failureType;
    private String failureMessage;

    public FilePipelineReplayExporter() {
        this(resolveConfiguredOutputFile());
    }

    public FilePipelineReplayExporter(Path configuredOutputFile) {
        this.configuredOutputFile = configuredOutputFile == null ? null : configuredOutputFile.toAbsolutePath().normalize();
    }

    @Override
    public synchronized void runStarted(
        String pipeline,
        Instant startedAt,
        PipelineReplayRunParameters runParameters,
        PipelineReplayTopology topology
    ) {
        this.pipeline = pipeline;
        this.startedAt = startedAt;
        this.runParameters = runParameters;
        this.topology = topology;
        this.durationMs = null;
        this.status = "running";
        this.failureType = null;
        this.failureMessage = null;
        this.events.clear();
        this.outputFile = resolveOutputFileForRun(pipeline, startedAt);
    }

    @Override
    public synchronized void emit(PipelineExecutionEvent event) {
        if (event != null) {
            events.add(event);
        }
    }

    @Override
    public synchronized void runCompleted(
        String pipeline,
        Instant startedAt,
        long durationMs,
        PipelineReplayTopology topology) {
        this.pipeline = pipeline;
        this.startedAt = startedAt;
        this.topology = topology;
        this.durationMs = durationMs;
        this.status = "completed";
        this.failureType = null;
        this.failureMessage = null;
        writeDocument();
    }

    @Override
    public synchronized void runFailed(
        String pipeline,
        Instant startedAt,
        long durationMs,
        PipelineReplayTopology topology,
        Throwable failure) {
        this.pipeline = pipeline;
        this.startedAt = startedAt;
        this.topology = topology;
        this.durationMs = durationMs;
        this.status = "failed";
        this.failureType = failure == null ? null : failure.getClass().getName();
        this.failureMessage = failure == null ? null : failure.getMessage();
        writeDocument();
    }

    @PreDestroy
    void flushOnShutdown() {
        synchronized (this) {
            if (!events.isEmpty() && durationMs != null) {
                writeDocument();
            }
        }
    }

    private void writeDocument() {
        if (outputFile == null || pipeline == null || startedAt == null || topology == null) {
            return;
        }
        try {
            Path parent = outputFile.toAbsolutePath().normalize().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            PipelineReplayDocument document = new PipelineReplayDocument(
                pipeline,
                startedAt,
                durationMs,
                status == null ? "completed" : status,
                failureType,
                failureMessage,
                runParameters,
                PipelineReplayTopologyAugmenter.augment(topology, events),
                List.copyOf(events));
            String json = PipelineJson.mapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(document);
            Files.writeString(outputFile, json + System.lineSeparator(), StandardCharsets.UTF_8);
            LOG.infof("Wrote replay JSON with %d events to %s.", events.size(), outputFile);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to write replay JSON to %s.", outputFile);
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
}
