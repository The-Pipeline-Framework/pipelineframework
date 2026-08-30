package org.pipelineframework.command;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;

/**
 * Builds command descriptors from runtime pipeline YAML.
 */
@ApplicationScoped
public class CommandStepDescriptorFactory {
    private static final int DESCRIPTOR_LOADER_THREADS = 4;
    private static final int DESCRIPTOR_LOADER_QUEUE_SIZE = 256;

    private final Map<String, CommandDescriptor> descriptors = new ConcurrentHashMap<>();
    private final AtomicInteger threadCounter = new AtomicInteger();
    private final ExecutorService blockingExecutor = new ThreadPoolExecutor(
        1,
        DESCRIPTOR_LOADER_THREADS,
        60L,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(DESCRIPTOR_LOADER_QUEUE_SIZE),
        r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("command-descriptor-loader-" + threadCounter.incrementAndGet());
            return t;
        });

    public Uni<CommandDescriptor> descriptor(
        String serviceName,
        String command,
        String inputType,
        String outputType,
        String commandIdGenerator
    ) {
        CommandDescriptor cached = descriptors.get(serviceName);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }
        return Uni.createFrom()
            .item(() -> {
                CommandDescriptor loaded = loadDescriptor(serviceName, command, inputType, outputType, commandIdGenerator);
                CommandDescriptor existing = descriptors.putIfAbsent(serviceName, loaded);
                return existing == null ? loaded : existing;
            })
            .runSubscriptionOn(blockingExecutor);
    }

    @PreDestroy
    void shutdown() {
        blockingExecutor.shutdown();
        try {
            if (!blockingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                blockingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            blockingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private CommandDescriptor loadDescriptor(
        String serviceName,
        String command,
        String inputType,
        String outputType,
        String commandIdGenerator
    ) {
        PipelineYamlConfig config = loadPipelineConfig(serviceName);
        PipelineYamlStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No command YAML step found for generated service " + serviceName));
        if (!"command".equalsIgnoreCase(step.kind())) {
            throw new IllegalStateException("Generated command service " + serviceName + " maps to non-command YAML step");
        }
        String resolvedCommand = firstNonBlank(step.command(), command)
            .orElseThrow(() -> new IllegalArgumentException("Command step " + serviceName + " is missing command"));
        String resolvedGenerator = firstNonBlank(step.commandIdGenerator(), commandIdGenerator)
            .orElseThrow(() -> new IllegalArgumentException(
                "Command step " + serviceName + " is missing commandIdGenerator"));
        Optional<NativeCommandSelector> selector = nativeSelector(resolvedCommand, step.commandConfig());
        Map<String, Object> configuration = selector.isPresent() ? operationConfiguration(step.commandConfig()) : step.commandConfig();
        return selector.map(value -> CommandDescriptor.nativeCommand(
                serviceName, value, inputType, outputType, resolvedGenerator,
                CommandDuplicatePolicy.fromString(step.duplicatePolicy()), configuration))
            .orElseGet(() -> new CommandDescriptor(
                serviceName, resolvedCommand, inputType, outputType, resolvedGenerator,
                CommandDuplicatePolicy.fromString(step.duplicatePolicy()), configuration));
    }

    private static PipelineYamlConfig loadPipelineConfig(String serviceName) {
        PipelineYamlConfigLoader loader = new PipelineYamlConfigLoader();
        Optional<Path> configPath = new PipelineYamlConfigLocator().locate(resolveConfigBase());
        if (configPath.isPresent()) {
            return loader.load(configPath.orElseThrow());
        }
        Optional<InputStream> resource = Stream.of(
                Thread.currentThread().getContextClassLoader(),
                CommandStepDescriptorFactory.class.getClassLoader())
            .filter(Objects::nonNull)
            .distinct()
            .map(classLoader -> new PipelineYamlConfigLocator().locateResource(classLoader))
            .flatMap(Optional::stream)
            .map(CommandStepDescriptorFactory::openResource)
            .findFirst();
        if (resource.isPresent()) {
            try (InputStream stream = resource.orElseThrow()) {
                return loader.load(stream);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read pipeline YAML for command step " + serviceName, e);
            }
        }
        throw new IllegalStateException("No pipeline YAML found for command step " + serviceName);
    }

    private static InputStream openResource(URL resource) {
        try {
            return resource.openStream();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open pipeline YAML resource " + resource, e);
        }
    }

    private static Path resolveConfigBase() {
        Optional<String> explicitConfig = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicitConfig.isPresent()) {
            String explicit = explicitConfig.get();
            Path candidate = Path.of(explicit);
            if (candidate.isAbsolute()) {
                return candidate.getParent() != null ? candidate.getParent() : candidate;
            }
        }
        return Path.of("").toAbsolutePath();
    }

    private static Optional<String> firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return Optional.of(value.trim());
            }
        }
        return Optional.empty();
    }

    private static Optional<NativeCommandSelector> nativeSelector(String command, Map<String, Object> configuration) {
        if (!command.startsWith("native:")) {
            return Optional.empty();
        }
        String provider = requiredString(configuration, "__tpf_native_provider");
        String operation = requiredString(configuration, "__tpf_native_operation");
        int providerVersion = requiredPositiveInteger(configuration, "__tpf_native_provider_version");
        int operationVersion = requiredPositiveInteger(configuration, "__tpf_native_operation_version");
        return Optional.of(new NativeCommandSelector(
            optionalString(configuration, "__tpf_native_binding").map(ConnectorBindingName::of),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of(provider), operation, ConnectorOperationKind.COMMAND, operationVersion),
            providerVersion,
            policy(configuration.get("__tpf_native_policy"))));
    }

    private static Optional<String> optionalString(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalArgumentException("native command selector " + key + " must be a non-blank string");
        }
        return Optional.of(string);
    }

    private static CommandPolicy policy(Object value) {
        if (value == null) {
            return CommandPolicy.none();
        }
        if (!(value instanceof Map<?, ?> values)) {
            throw new IllegalArgumentException("native command policy must be a map");
        }
        values.keySet().stream()
            .map(String::valueOf)
            .filter(key -> !Set.of(
                "requireRetryRedrive", "requireIdempotency", "requireReconciliation",
                "requiredExecutionPosture", "minimumMachineConfirmation",
                "requireUserConfirmation").contains(key))
            .sorted()
            .findFirst()
            .ifPresent(key -> {
                throw new IllegalArgumentException("native command policy has unsupported field '" + key + "'");
            });
        return new CommandPolicy(
            bool(values, "requireRetryRedrive"),
            bool(values, "requireIdempotency"),
            bool(values, "requireReconciliation"),
            optionalEnum(values, "requiredExecutionPosture", CommandExecutionPosture.class),
            optionalEnum(values, "minimumMachineConfirmation", CommandMachineConfirmation.class),
            bool(values, "requireUserConfirmation"));
    }

    private static Map<String, Object> operationConfiguration(Map<String, Object> configuration) {
        Map<String, Object> result = new LinkedHashMap<>();
        configuration.forEach((key, value) -> {
            if (!key.startsWith("__tpf_native_")) {
                result.put(key, value);
            }
        });
        return Map.copyOf(result);
    }

    private static String requiredString(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalArgumentException("native command selector " + key + " must be a non-blank string");
        }
        return string;
    }

    private static int requiredPositiveInteger(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof Number number) || number.intValue() < 1 || number.doubleValue() != number.intValue()) {
            throw new IllegalArgumentException("native command selector " + key + " must be a positive integer");
        }
        return number.intValue();
    }

    private static boolean bool(Map<?, ?> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException("native command policy " + key + " must be a boolean");
    }

    private static <T extends Enum<T>> Optional<T> optionalEnum(Map<?, ?> values, String key, Class<T> type) {
        Object value = values.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (!(value instanceof String string)) {
            throw new IllegalArgumentException("native command policy " + key + " must be a string");
        }
        try {
            return Optional.of(Enum.valueOf(type, string));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("native command policy " + key + " has unsupported value " + string, exception);
        }
    }

    private static String toServiceName(String stepName) {
        if (stepName == null || stepName.isBlank()) {
            return "ProcessStepService";
        }
        String stripped = stepName.startsWith("Process ") ? stepName.substring("Process ".length()) : stepName;
        StringBuilder formatted = new StringBuilder();
        for (String part : stripped.split(" ")) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(java.util.Locale.ROOT);
            formatted.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                formatted.append(lower.substring(1));
            }
        }
        return formatted.isEmpty() ? "ProcessStepService" : "Process" + formatted + "Service";
    }
}
