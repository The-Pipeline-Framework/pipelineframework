package org.pipelineframework.objectingest;

import java.nio.file.Path;
import java.util.Optional;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/**
 * Thin Quarkus lifecycle adapter for the runtime-neutral object ingest runner.
 */
@ApplicationScoped
public class ObjectIngestBootstrap {

    private static final Logger LOG = Logger.getLogger(ObjectIngestBootstrap.class);

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineExecutionService executionService;

    @Inject
    Instance<ObjectIngestTelemetry> telemetry;

    private volatile ObjectIngestRunner runner;

    void onStartup(@Observes StartupEvent event) {
        if (!autostartEnabled()) {
            LOG.info("Object ingest autostart disabled; waiting for explicit runner invocation.");
            return;
        }
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            LOG.debugf("Object ingest skipped: orchestrator mode is %s (requires QUEUE_ASYNC)", orchestratorConfig.mode());
            return;
        }
        Optional<Path> configPath = resolveConfigPath();
        if (configPath.isEmpty()) {
            return;
        }
        PipelineYamlConfig config;
        ObjectSourceRegistry registry;
        try {
            config = new PipelineYamlConfigLoader().load(configPath.get());
            registry = ObjectSourceRegistry.load();
        } catch (RuntimeException e) {
            LOG.warnf(e, "Object ingest disabled: failed to initialize from pipeline config %s", configPath.get());
            return;
        }
        if (config.input() == null || config.input().object() == null) {
            return;
        }
        ObjectIngestRunner objectRunner = new ObjectIngestRunner(
            config,
            registry,
            (input, tenantId, idempotencyKey) -> executionService.executePipelineAsync(input, tenantId, idempotencyKey),
            telemetry.isResolvable() ? telemetry.get() : ObjectIngestTelemetry.NOOP);
        try {
            objectRunner.start();
            runner = objectRunner;
        } catch (RuntimeException e) {
            objectRunner.close();
            throw e;
        }
    }

    @PreDestroy
    synchronized void shutdown() {
        ObjectIngestRunner active = runner;
        if (active != null) {
            active.close();
            runner = null;
        }
    }

    private Optional<Path> resolveConfigPath() {
        Optional<String> explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit.isPresent()) {
            return Optional.of(Path.of(explicit.get()).toAbsolutePath().normalize());
        }
        try {
            return new PipelineYamlConfigLocator().locate(Path.of(System.getProperty("user.dir")));
        } catch (RuntimeException e) {
            LOG.debugf(e, "Object ingest skipped because pipeline config could not be located.");
            return Optional.empty();
        }
    }

    private Optional<String> firstNonBlank(String primary, String fallback) {
        if (primary != null && !primary.isBlank()) {
            return Optional.of(primary.trim());
        }
        return fallback == null || fallback.isBlank() ? Optional.empty() : Optional.of(fallback.trim());
    }

    private boolean autostartEnabled() {
        if (processCommandRequestsIngestOnce()) {
            return false;
        }
        return firstNonBlank(
                System.getProperty("pipeline.object-ingest.autostart"),
                System.getenv("PIPELINE_OBJECT_INGEST_AUTOSTART"))
            .map(Boolean::parseBoolean)
            .orElse(true);
    }

    private boolean processCommandRequestsIngestOnce() {
        String command = System.getProperty("sun.java.command", "");
        if (command == null || command.isBlank()) {
            return false;
        }
        return java.util.Arrays.asList(command.trim().split("\\s+")).contains("--ingest-once");
    }
}
