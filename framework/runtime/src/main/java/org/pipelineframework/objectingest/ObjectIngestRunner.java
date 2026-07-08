package org.pipelineframework.objectingest;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;
import org.pipelineframework.config.boundary.PipelineObjectInputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;

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
    private volatile List<ObjectIngestInputAdapter<?, ?>> resolvedInputAdapters;

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

    public static Optional<ObjectIngestRunner> loadFromDefaultConfig(
        ObjectExecutionAdmission admission,
        ObjectIngestTelemetry telemetry
    ) {
        Optional<java.nio.file.Path> configPath = locateConfig();
        if (configPath.isEmpty()) {
            return Optional.empty();
        }
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath.get());
        if (config.input() == null || config.input().object() == null) {
            return Optional.empty();
        }
        return Optional.of(new ObjectIngestRunner(
            config,
            ObjectSourceRegistry.load(),
            admission,
            telemetry));
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
        java.util.ArrayList<String> executionIds = new java.util.ArrayList<>();
        for (ObjectSourceItem item : items) {
            try {
                ObjectSnapshot snapshot = snapshot(source, provider, item);
                Object domainInput = mapper().map(snapshot);
                Object pipelineInput = adaptForAdmission(domainInput);
                String idempotencyKey = ObjectIdentity.executionKey(source.name(), snapshot, source.identity());
                org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto accepted =
                    admission.submit(pipelineInput, DEFAULT_TENANT_ID, idempotencyKey).await().atMost(Duration.ofSeconds(30));
                if (accepted == null) {
                    throw new IllegalStateException("Object execution admission returned null accepted response");
                }
                if (accepted.executionId() != null && !accepted.executionId().isBlank()) {
                    executionIds.add(accepted.executionId());
                }
                if (accepted.duplicate()) {
                    telemetry.duplicate(source.name(), source.provider(), item.key());
                } else {
                    telemetry.submitted(source.name(), source.provider(), item.key());
                }
                submitted++;
            } catch (RuntimeException e) {
                telemetry.failed(source.name(), source.provider(), item.key(), e);
                LOG.warnf(e, "Object ingest failed for source=%s key=%s", source.name(), item.key());
                failed++;
            }
        }
        return new PollResult(items.size(), submitted, failed, List.copyOf(executionIds));
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

    private Object adaptForAdmission(Object domainInput) {
        Objects.requireNonNull(domainInput, "Object Ingest mapped input must not be null");
        for (ObjectIngestInputAdapter<?, ?> adapter : inputAdapters()) {
            Class<?> domainType = adapter.domainType();
            if (domainType != null && domainType.isInstance(domainInput)) {
                return adaptWith(adapter, domainInput);
            }
        }
        return domainInput;
    }

    @SuppressWarnings("unchecked")
    private static Object adaptWith(ObjectIngestInputAdapter<?, ?> adapter, Object domainInput) {
        return ((ObjectIngestInputAdapter<Object, Object>) adapter).toPipelineInput(domainInput);
    }

    @SuppressWarnings("rawtypes")
    private List<ObjectIngestInputAdapter<?, ?>> inputAdapters() {
        List<ObjectIngestInputAdapter<?, ?>> adapters = resolvedInputAdapters;
        if (adapters != null) {
            return adapters;
        }
        synchronized (this) {
            adapters = resolvedInputAdapters;
            if (adapters != null) {
                return adapters;
            }
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            ServiceLoader<ObjectIngestInputAdapter> serviceLoader = loader == null
                ? ServiceLoader.load(ObjectIngestInputAdapter.class)
                : ServiceLoader.load(ObjectIngestInputAdapter.class, loader);
            java.util.ArrayList<ObjectIngestInputAdapter<?, ?>> loaded = new java.util.ArrayList<>();
            for (ObjectIngestInputAdapter<?, ?> adapter : serviceLoader) {
                loaded.add(adapter);
            }
            resolvedInputAdapters = List.copyOf(loaded);
            return resolvedInputAdapters;
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

    public record PollResult(int listed, int submitted, int failed, List<String> executionIds) {
    }

    private static Optional<java.nio.file.Path> locateConfig() {
        Optional<String> explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit.isPresent()) {
            return Optional.of(java.nio.file.Path.of(explicit.get()).toAbsolutePath().normalize());
        }
        try {
            return new PipelineYamlConfigLocator().locate(java.nio.file.Path.of(System.getProperty("user.dir")));
        } catch (RuntimeException e) {
            LOG.debugf(e, "Object ingest pipeline config discovery failed");
            return Optional.empty();
        }
    }

    private static Optional<String> firstNonBlank(String primary, String fallback) {
        if (primary != null && !primary.isBlank()) {
            return Optional.of(primary.trim());
        }
        return fallback == null || fallback.isBlank() ? Optional.empty() : Optional.of(fallback.trim());
    }
}
