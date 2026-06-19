package org.pipelineframework.objectingest;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;
import org.pipelineframework.config.boundary.PipelineObjectInputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;

/**
 * Runtime-neutral listing poller and async admission engine for object sources.
 */
public final class ObjectIngestRunner implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ObjectIngestRunner.class);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
    private static final String DEFAULT_TENANT_ID = null;

    private final PipelineYamlConfig config;
    private final ObjectSourceRegistry registry;
    private final ObjectExecutionAdmission admission;
    private final ObjectIngestTelemetry telemetry;
    private final ScheduledExecutorService executor;
    private final boolean ownsExecutor;
    private volatile ScheduledFuture<?> future;
    private volatile ObjectSnapshotMapper<Object> resolvedMapper;

    public ObjectIngestRunner(
        PipelineYamlConfig config,
        ObjectSourceRegistry registry,
        ObjectExecutionAdmission admission,
        ObjectIngestTelemetry telemetry
    ) {
        this(
            config,
            registry,
            admission,
            telemetry,
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "tpf-object-ingest");
                thread.setDaemon(true);
                return thread;
            }),
            true);
    }

    public ObjectIngestRunner(
        PipelineYamlConfig config,
        ObjectSourceRegistry registry,
        ObjectExecutionAdmission admission,
        ObjectIngestTelemetry telemetry,
        ScheduledExecutorService executor,
        boolean ownsExecutor
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.telemetry = telemetry == null ? ObjectIngestTelemetry.NOOP : telemetry;
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ownsExecutor = ownsExecutor;
    }

    public boolean enabled() {
        return objectInput()
            .map(input -> config.sources().get(input.source()))
            .map(source -> source.poll().enabled())
            .orElse(false);
    }

    public synchronized void start() {
        if (!enabled()) {
            return;
        }
        if (future != null) {
            LOG.debug("Object ingest runner already started");
            return;
        }
        Duration interval = source().poll().interval();
        long intervalMs = Math.max(1000L, interval.toMillis());
        future = executor.scheduleWithFixedDelay(this::pollSafely, 0L, intervalMs, TimeUnit.MILLISECONDS);
        LOG.infof("Object ingest enabled for source=%s provider=%s interval=%s",
            source().name(), source().provider(), interval);
    }

    public PollResult pollOnce() {
        PipelineObjectSourceConfig source = source();
        ObjectSourceProvider provider = registry.require(source.provider());
        List<ObjectSourceItem> items = provider.list(source, source.poll().batchSize());
        telemetry.listed(source.name(), source.provider(), items.size());
        int submitted = 0;
        int failed = 0;
        for (ObjectSourceItem item : items) {
            try {
                ObjectSnapshot snapshot = snapshot(source, provider, item);
                Object domainInput = mapper().map(snapshot);
                String idempotencyKey = ObjectIdentity.executionKey(source.name(), snapshot, source.identity());
                admission.submit(domainInput, DEFAULT_TENANT_ID, idempotencyKey).await().atMost(Duration.ofSeconds(30));
                telemetry.submitted(source.name(), source.provider(), item.key());
                submitted++;
            } catch (RuntimeException e) {
                telemetry.failed(source.name(), source.provider(), item.key(), e);
                LOG.warnf(e, "Object ingest failed for source=%s key=%s", source.name(), item.key());
                failed++;
            }
        }
        return new PollResult(items.size(), submitted, failed);
    }

    @Override
    public synchronized void close() {
        ScheduledFuture<?> active = future;
        if (active != null) {
            active.cancel(false);
            future = null;
        }
        if (!ownsExecutor) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private void pollSafely() {
        try {
            pollOnce();
        } catch (RuntimeException e) {
            LOG.warnf(e, "Object ingest poll failed for source=%s", source().name());
        }
    }

    private ObjectSnapshot snapshot(
        PipelineObjectSourceConfig source,
        ObjectSourceProvider provider,
        ObjectSourceItem item
    ) {
        PipelineObjectPayloadConfig payload = source.payload();
        Optional<String> text = "text".equalsIgnoreCase(payload.mode())
            ? provider.readText(source, item, payload.maxBytes())
            : Optional.empty();
        return item.toSnapshot(source.name(), text.orElse(null));
    }

    private ObjectSnapshotMapper<Object> mapper() {
        PipelineObjectInputConfig input = objectInput()
            .orElseThrow(() -> new IllegalStateException("pipeline input object binding is not configured"));
        ObjectSnapshotMapper<Object> mapper = resolvedMapper;
        if (mapper != null) {
            return mapper;
        }
        synchronized (this) {
            mapper = resolvedMapper;
            if (mapper != null) {
                return mapper;
            }
            resolvedMapper = createMapper(input);
            return resolvedMapper;
        }
    }

    @SuppressWarnings("unchecked")
    private ObjectSnapshotMapper<Object> createMapper(PipelineObjectInputConfig input) {
        String mapperClassName = ObjectText.normalize(input.mapper())
            .orElseThrow(() -> new IllegalStateException("Object input mapper must not be blank"));
        validateMapperClassName(mapperClassName);
        try {
            Class<?> mapperClass = Class.forName(mapperClassName, true, Thread.currentThread().getContextClassLoader());
            Object mapper = mapperClass.getDeclaredConstructor().newInstance();
            if (!(mapper instanceof ObjectSnapshotMapper<?> snapshotMapper)) {
                throw new IllegalStateException(
                    "Object input mapper '" + mapperClassName + "' must implement " + ObjectSnapshotMapper.class.getName());
            }
            return (ObjectSnapshotMapper<Object>) snapshotMapper;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create object input mapper: " + mapperClassName, e);
        }
    }

    private void validateMapperClassName(String mapperClassName) {
        String basePackage = ObjectText.normalize(config.basePackage())
            .orElseThrow(() -> new IllegalStateException(
                "Object input mapper requires pipeline basePackage for runtime class loading"));
        if (!mapperClassName.startsWith(basePackage + ".")) {
            throw new IllegalStateException(
                "Object input mapper '" + mapperClassName + "' must be under basePackage '" + basePackage + "'");
        }
    }

    private Optional<PipelineObjectInputConfig> objectInput() {
        return config.input() == null ? Optional.empty() : Optional.ofNullable(config.input().object());
    }

    private PipelineObjectSourceConfig source() {
        PipelineObjectInputConfig input = objectInput()
            .orElseThrow(() -> new IllegalStateException("pipeline input object binding is not configured"));
        PipelineObjectSourceConfig source = config.sources().get(input.source());
        if (source == null) {
            throw new IllegalStateException("input object source not found: " + input.source());
        }
        return source;
    }

    public record PollResult(int listed, int submitted, int failed) {
    }
}
