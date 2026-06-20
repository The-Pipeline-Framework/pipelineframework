package org.pipelineframework.objectpublish;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;

/**
 * Runtime-neutral terminal object publish engine.
 */
public final class ObjectPublishRunner {

    private static final Logger LOG = Logger.getLogger(ObjectPublishRunner.class);
    private static final Pattern JAVA_CLASS_NAME = Pattern.compile("[a-zA-Z0-9._$]+");
    private static final ObjectPublishRunner DISABLED =
        new ObjectPublishRunner(null, new ObjectTargetRegistry(List.of()), ObjectPublishTelemetry.NOOP, true);

    private final PipelineYamlConfig config;
    private final ObjectTargetRegistry registry;
    private final ObjectPublishTelemetry telemetry;
    private final boolean disabled;
    private volatile ObjectPublishMapper<Object> resolvedMapper;

    public ObjectPublishRunner(
        PipelineYamlConfig config,
        ObjectTargetRegistry registry,
        ObjectPublishTelemetry telemetry
    ) {
        this(config, registry, telemetry, false);
    }

    private ObjectPublishRunner(
        PipelineYamlConfig config,
        ObjectTargetRegistry registry,
        ObjectPublishTelemetry telemetry,
        boolean disabled
    ) {
        this.config = config;
        this.registry = Objects.requireNonNull(registry, "registry");
        this.telemetry = telemetry == null ? ObjectPublishTelemetry.NOOP : telemetry;
        this.disabled = disabled;
    }

    public static ObjectPublishRunner disabled() {
        return DISABLED;
    }

    public static ObjectPublishRunner loadFromDefaultConfig() {
        Optional<Path> configPath = locateConfig();
        if (configPath.isEmpty()) {
            return disabled();
        }
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath.get());
        return new ObjectPublishRunner(config, ObjectTargetRegistry.load(), ObjectPublishTelemetry.NOOP);
    }

    public boolean enabled() {
        return !disabled && objectOutput().isPresent();
    }

    public Object publish(Object terminalOutput) {
        if (!enabled()) {
            return terminalOutput;
        }
        if (terminalOutput instanceof Multi<?> multi) {
            return publishMulti(multi);
        }
        if (terminalOutput instanceof Uni<?> uni) {
            return publishUni(uni);
        }
        return terminalOutput;
    }

    private Multi<?> publishMulti(Multi<?> multi) {
        return multi.collect().asList()
            .onItem().transformToUni(items -> publishItems(items).replaceWith(items))
            .onItem().transformToMulti(items -> Multi.createFrom().iterable(items));
    }

    private Uni<?> publishUni(Uni<?> uni) {
        return uni.onItem().transformToUni(item -> {
            if (item == null) {
                return Uni.createFrom().nullItem();
            }
            return publishItems(List.of(item)).replaceWith(item);
        });
    }

    public Uni<Void> publishItems(List<?> items) {
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        PipelineObjectPublishConfig target = target(output);
        if (items == null || items.isEmpty()) {
            telemetry.skipped(target.name());
            return Uni.createFrom().voidItem();
        }
        Map<String, List<Object>> groups = group(items);
        telemetry.grouped(target.name(), items.size(), groups.size());
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Map.Entry<String, List<Object>> entry : groups.entrySet()) {
            chain = chain.chain(() -> writeGroup(target, entry.getKey(), entry.getValue()));
        }
        return chain;
    }

    private Map<String, List<Object>> group(List<?> items) {
        Map<String, List<Object>> groups = new LinkedHashMap<>();
        ObjectPublishMapper<Object> mapper = mapper();
        for (Object item : items) {
            String groupKey = normalize(mapper.groupKey(item))
                .orElseThrow(() -> new IllegalStateException("Object publish mapper returned a blank group key"));
            groups.computeIfAbsent(groupKey, ignored -> new java.util.ArrayList<>()).add(item);
        }
        return groups;
    }

    private Uni<Void> writeGroup(PipelineObjectPublishConfig target, String groupKey, List<Object> items) {
        ObjectPublishMapper<Object> mapper = mapper();
        Map<String, String> labels = mapper.labels(groupKey, List.copyOf(items));
        if (labels == null) {
            labels = Map.of();
        }
        ObjectPayload payload = mapper.render(groupKey, List.copyOf(items));
        if (payload == null) {
            throw new IllegalStateException("Object publish mapper returned null payload for group: " + groupKey);
        }
        byte[] bytes = payload.bytes();
        String contentType = normalize(payload.contentType()).orElse(target.payload().contentType());
        String objectKey = renderKey(target.naming().keyTemplate(), groupKey, labels);
        String checksum = sha256(bytes);
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("target", target.name());
        metadata.put("groupKey", groupKey);
        metadata.putAll(labels);
        metadata.putAll(payload.metadata());
        ObjectWriteRequest request = new ObjectWriteRequest(
            target.name(),
            target,
            objectKey,
            bytes,
            contentType,
            metadata,
            checksum,
            "object-publish:" + target.name() + ":" + objectKey + ":" + checksum);
        ObjectTargetProvider provider = registry.require(target.provider());
        return provider.write(request)
            .invoke(result -> telemetry.published(target.name(), target.provider(), objectKey, result.bytes()))
            .onFailure().invoke(failure -> {
                telemetry.failed(target.name(), target.provider(), objectKey, failure);
                LOG.warnf(failure, "Object publish failed for target=%s key=%s", target.name(), objectKey);
            })
            .replaceWithVoid();
    }

    private String renderKey(String template, String groupKey, Map<String, String> labels) {
        String rendered = template == null ? "{groupKey}" : template;
        rendered = rendered.replace("{groupKey}", groupKey);
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            rendered = rendered.replace("{" + entry.getKey() + "}", entry.getValue());
        }
        return normalize(rendered)
            .orElseThrow(() -> new IllegalStateException("Object publish rendered a blank object key"));
    }

    private ObjectPublishMapper<Object> mapper() {
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        ObjectPublishMapper<Object> mapper = resolvedMapper;
        if (mapper != null) {
            return mapper;
        }
        synchronized (this) {
            mapper = resolvedMapper;
            if (mapper != null) {
                return mapper;
            }
            resolvedMapper = createMapper(output);
            return resolvedMapper;
        }
    }

    @SuppressWarnings("unchecked")
    private ObjectPublishMapper<Object> createMapper(PipelineObjectOutputConfig output) {
        String mapperClassName = normalize(output.mapper())
            .orElseThrow(() -> new IllegalStateException("Object output mapper must not be blank"));
        validateMapperClassName(mapperClassName);
        try {
            Class<?> mapperClass = Class.forName(mapperClassName, true, Thread.currentThread().getContextClassLoader());
            Object mapper = mapperClass.getDeclaredConstructor().newInstance();
            if (!(mapper instanceof ObjectPublishMapper<?> publishMapper)) {
                throw new IllegalStateException(
                    "Object output mapper '" + mapperClassName + "' must implement " + ObjectPublishMapper.class.getName());
            }
            return (ObjectPublishMapper<Object>) publishMapper;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create object output mapper: " + mapperClassName, e);
        }
    }

    private void validateMapperClassName(String mapperClassName) {
        String basePackage = normalize(config.basePackage())
            .orElseThrow(() -> new IllegalStateException(
                "Object output mapper requires pipeline basePackage for runtime class loading"));
        if (!JAVA_CLASS_NAME.matcher(mapperClassName).matches()
            || mapperClassName.contains("..")
            || mapperClassName.contains("/")
            || mapperClassName.contains("\\")) {
            throw new IllegalStateException("Object output mapper class name is invalid: " + mapperClassName);
        }
        if (!mapperClassName.startsWith(basePackage + ".")) {
            throw new IllegalStateException(
                "Object output mapper '" + mapperClassName + "' must be under basePackage '" + basePackage + "'");
        }
    }

    private Optional<PipelineObjectOutputConfig> objectOutput() {
        return config == null || config.output() == null
            ? Optional.empty()
            : Optional.ofNullable(config.output().object());
    }

    private PipelineObjectPublishConfig target(PipelineObjectOutputConfig output) {
        PipelineObjectPublishConfig target = config.publish().get(output.target());
        if (target == null) {
            throw new IllegalStateException("output object publish target not found: " + output.target());
        }
        return target;
    }

    private static Optional<Path> locateConfig() {
        Optional<String> explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit.isPresent()) {
            return Optional.of(Path.of(explicit.get()).toAbsolutePath().normalize());
        }
        try {
            return new PipelineYamlConfigLocator().locate(Path.of(System.getProperty("user.dir")));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Optional<String> firstNonBlank(String primary, String fallback) {
        if (primary != null && !primary.isBlank()) {
            return Optional.of(primary.trim());
        }
        if (fallback != null && !fallback.isBlank()) {
            return Optional.of(fallback.trim());
        }
        return Optional.empty();
    }

    private static String sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }

    private static Optional<String> normalize(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }
}
