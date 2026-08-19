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
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.config.boundary.PipelineObjectInputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectSelectionConfig;
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
    private final Optional<ConnectorBindingRegistry> connectorBindings;
    private final ScheduledExecutorService executor;
    private final boolean ownsExecutor;
    private final AtomicBoolean pollInProgress = new AtomicBoolean();
    private volatile ScheduledFuture<?> future;
    private volatile ObjectSnapshotMapper<Object> resolvedMapper;
    private volatile List<ObjectIngestInputAdapter<?, ?>> resolvedInputAdapters;
    private volatile List<ObjectSelectionMapper<?>> resolvedSelectionMappers;

    public ObjectIngestRunner(
        PipelineYamlConfig config,
        ObjectSourceRegistry registry,
        ObjectExecutionAdmission admission,
        ObjectIngestTelemetry telemetry
    ) {
        this(config, registry, admission, telemetry, Optional.empty(),
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "tpf-object-ingest");
                thread.setDaemon(true);
                return thread;
            }), true);
    }

    public ObjectIngestRunner(
        PipelineYamlConfig config,
        ObjectSourceRegistry registry,
        ObjectExecutionAdmission admission,
        ObjectIngestTelemetry telemetry,
        ConnectorBindingRegistry connectorBindings
    ) {
        this(
            config,
            registry,
            admission,
            telemetry,
            Optional.of(Objects.requireNonNull(connectorBindings, "connectorBindings")),
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
        this(config, registry, admission, telemetry, Optional.empty(), executor, ownsExecutor);
    }

    public ObjectIngestRunner(
        PipelineYamlConfig config,
        ObjectSourceRegistry registry,
        ObjectExecutionAdmission admission,
        ObjectIngestTelemetry telemetry,
        ConnectorBindingRegistry connectorBindings,
        ScheduledExecutorService executor,
        boolean ownsExecutor
    ) {
        this(config, registry, admission, telemetry,
            Optional.of(Objects.requireNonNull(connectorBindings, "connectorBindings")), executor, ownsExecutor);
    }

    private ObjectIngestRunner(
        PipelineYamlConfig config,
        ObjectSourceRegistry registry,
        ObjectExecutionAdmission admission,
        ObjectIngestTelemetry telemetry,
        Optional<ConnectorBindingRegistry> connectorBindings,
        ScheduledExecutorService executor,
        boolean ownsExecutor
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.telemetry = telemetry == null ? ObjectIngestTelemetry.NOOP : telemetry;
        this.connectorBindings = Objects.requireNonNull(connectorBindings, "connectorBindings");
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

    public Uni<PollResult> pollOnce() {
        return Uni.createFrom().item(() -> {
            PipelineObjectSourceConfig source = source();
            ObjectSourceProvider provider = registry.require(source.provider());
            List<ObjectSourceItem> items = provider.list(source, source.poll().batchSize());
            telemetry.listed(source.name(), source.provider(), items.size());
            return new ListedObjects(source, provider, items);
        }).runSubscriptionOn(executor).onItem().transformToUni(listed -> {
            Optional<PipelineObjectSelectionConfig> selection = objectInput().orElseThrow().selection();
            return selection.isPresent()
                ? pollSelection(listed.source(), listed.provider(), listed.items(), selection.orElseThrow())
                : pollItems(listed.source(), listed.provider(), listed.items());
        });
    }

    private Uni<PollResult> pollItems(PipelineObjectSourceConfig source, ObjectSourceProvider provider,
                                      List<ObjectSourceItem> items) {
        return Multi.createFrom().iterable(items)
            .onItem().transformToUniAndConcatenate(item -> admitItem(source, provider, item))
            .collect().asList()
            .map(results -> new PollResult(
                items.size(),
                Math.toIntExact(results.stream().filter(AdmissionResult::submitted).count()),
                Math.toIntExact(results.stream().filter(result -> !result.submitted()).count()),
                results.stream().flatMap(result -> result.executionId().stream()).toList()));
    }

    private Uni<AdmissionResult> admitItem(PipelineObjectSourceConfig source, ObjectSourceProvider provider,
                                            ObjectSourceItem item) {
        return Uni.createFrom().item(() -> {
            ObjectSnapshot snapshot = snapshot(source, provider, item);
            Object domainInput = mapper().map(snapshot);
            Object pipelineInput = adaptForAdmission(domainInput);
            String idempotencyKey = ObjectIdentity.executionKey(source.name(), snapshot, source.identity());
            return new PreparedAdmission(pipelineInput, idempotencyKey);
        }).runSubscriptionOn(executor)
            .onItem().transformToUni(prepared ->
                admission.submit(prepared.pipelineInput(), DEFAULT_TENANT_ID, prepared.idempotencyKey()))
            .map(accepted -> {
                requireAccepted(accepted);
                if (accepted.duplicate()) {
                    telemetry.duplicate(source.name(), source.provider(), item.key());
                } else {
                    telemetry.submitted(source.name(), source.provider(), item.key());
                }
                Optional<String> executionId = ObjectText.normalize(accepted.executionId());
                return new AdmissionResult(true, executionId);
            })
            .onFailure().recoverWithItem(failure -> {
                telemetry.failed(source.name(), source.provider(), item.key(), failure);
                LOG.warnf(failure, "Object ingest failed for source=%s key=%s", source.name(), item.key());
                return new AdmissionResult(false, Optional.empty());
            });
    }

    private Uni<PollResult> pollSelection(PipelineObjectSourceConfig source, ObjectSourceProvider provider,
                                          List<ObjectSourceItem> listed, PipelineObjectSelectionConfig selection) {
        if (listed.isEmpty()) {
            return Uni.createFrom().item(new PollResult(0, 0, 0, List.of()));
        }
        if (!selection.keys().isEmpty()) {
            java.util.Set<String> listedKeys = listed.stream().map(ObjectSourceItem::key).collect(java.util.stream.Collectors.toSet());
            if (!listedKeys.containsAll(selection.keys().keySet())) {
                return Uni.createFrom().item(new PollResult(listed.size(), 0, 0, List.of()));
            }
        }
        List<ObjectSourceItem> selected = listed.stream()
            .filter(item -> selection.keys().isEmpty() || selection.keys().containsValue(item.key()))
            .sorted(java.util.Comparator.comparing(ObjectSourceItem::key))
            .toList();
        if (selected.isEmpty()) {
            return Uni.createFrom().item(new PollResult(listed.size(), 0, 0, List.of()));
        }
        return Uni.createFrom().item(() -> {
            List<ObjectSnapshot> snapshots = selected.stream()
                .map(item -> snapshot(source, provider, item))
                .toList();
            Object domainInput = selectionMapper().map(snapshots);
            Object pipelineInput = adaptForAdmission(domainInput);
            String idempotencyKey = ObjectIdentity.executionKey(source.name(), snapshots, source.identity());
            return new PreparedAdmission(pipelineInput, idempotencyKey);
        }).runSubscriptionOn(executor)
            .onItem().transformToUni(prepared ->
                admission.submit(prepared.pipelineInput(), DEFAULT_TENANT_ID, prepared.idempotencyKey()))
            .map(accepted -> {
                requireAccepted(accepted);
                String selectionKey = selected.stream().map(ObjectSourceItem::key)
                    .collect(java.util.stream.Collectors.joining(","));
                if (accepted.duplicate()) {
                    telemetry.duplicate(source.name(), source.provider(), selectionKey);
                } else {
                    telemetry.submitted(source.name(), source.provider(), selectionKey);
                }
                List<String> executionIds = accepted.executionId() == null || accepted.executionId().isBlank()
                    ? List.of() : List.of(accepted.executionId());
                return new PollResult(listed.size(), 1, 0, executionIds);
            }).onFailure().recoverWithItem(failure -> {
                telemetry.failed(source.name(), source.provider(), "selection", failure);
                LOG.warnf(failure, "Grouped Object Ingest failed for source=%s", source.name());
                return new PollResult(listed.size(), 0, 1, List.of());
            });
    }

    private static void requireAccepted(org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto accepted) {
        if (accepted == null) {
            throw new IllegalStateException("Object execution admission returned null accepted response");
        }
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
        if (!pollInProgress.compareAndSet(false, true)) {
            LOG.debugf("Object ingest poll still active for source=%s", source().name());
            return;
        }
        pollOnce().subscribe().with(
            ignored -> pollInProgress.set(false),
            failure -> {
                pollInProgress.set(false);
                LOG.warnf(failure, "Object ingest poll failed for source=%s", source().name());
            });
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
        ObjectSnapshot snapshot = item.toSnapshot(source.name(), text.orElse(null));
        if (source.binding().isEmpty() || snapshot.contentRef() == null) {
            return snapshot;
        }
        if (connectorBindings.isEmpty()) {
            throw new IllegalStateException(
                "object source '" + source.name() + "' declares binding '" + source.binding().orElseThrow()
                    + "' but the connector binding registry is unavailable");
        }
        return snapshot.withContentRef(connectorBindings.orElseThrow().ownPayloadReference(
            ConnectorBindingName.of(source.binding().orElseThrow()), provider.id(), provider.majorVersion(),
            snapshot.contentRef()));
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
    private ObjectSelectionMapper<Object> selectionMapper() {
        PipelineObjectInputConfig input = objectInput()
            .orElseThrow(() -> new IllegalStateException("pipeline input object binding is not configured"));
        return (ObjectSelectionMapper<Object>) selectionMappers().stream()
            .filter(mapper -> mapper.outputType().getName().equals(input.type())
                || mapper.outputType().getSimpleName().equals(input.typeName()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No generated Object Selection mapper is available for " + input.type()));
    }

    @SuppressWarnings("rawtypes")
    private List<ObjectSelectionMapper<?>> selectionMappers() {
        List<ObjectSelectionMapper<?>> mappers = resolvedSelectionMappers;
        if (mappers != null) {
            return mappers;
        }
        synchronized (this) {
            mappers = resolvedSelectionMappers;
            if (mappers != null) {
                return mappers;
            }
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            ServiceLoader<ObjectSelectionMapper> serviceLoader = loader == null
                ? ServiceLoader.load(ObjectSelectionMapper.class)
                : ServiceLoader.load(ObjectSelectionMapper.class, loader);
            java.util.ArrayList<ObjectSelectionMapper<?>> loaded = new java.util.ArrayList<>();
            for (ObjectSelectionMapper<?> mapper : serviceLoader) {
                loaded.add(mapper);
            }
            resolvedSelectionMappers = List.copyOf(loaded);
            return resolvedSelectionMappers;
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
        String mapperClassName = input.mapper().flatMap(ObjectText::normalize)
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

    private record ListedObjects(PipelineObjectSourceConfig source, ObjectSourceProvider provider,
                                 List<ObjectSourceItem> items) {
    }

    private record PreparedAdmission(Object pipelineInput, String idempotencyKey) {
    }

    private record AdmissionResult(boolean submitted, Optional<String> executionId) {
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
