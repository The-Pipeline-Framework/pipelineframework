package org.pipelineframework.dispatch;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.config.pipeline.PipelineResources;
import org.pipelineframework.config.pipeline.PipelineYamlCallable;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;

/** Lands compiler-pinned callable facts into the runtime descriptor used by a generated invocation adapter. */
@ApplicationScoped
public final class OperationDispatchDescriptorFactory {
    public OperationDispatchDescriptor descriptor(String serviceName) {
        Path base = resolveConfigBase();
        Path configPath = new PipelineYamlConfigLocator().locate(base)
            .orElseThrow(() -> new IllegalStateException(
                "No pipeline YAML found for dynamic operation service " + serviceName));
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        List<PipelineYamlStep> owner = config.stepDefinitions().values().stream()
            .filter(steps -> steps.stream().anyMatch(candidate -> serviceName.equals(toServiceName(candidate.name()))))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No dynamic operation YAML step found for generated service " + serviceName));
        PipelineYamlStep dispatch = owner.stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst().orElseThrow();
        String name = dispatch.dynamicOperation().orElseThrow(() -> new IllegalStateException(
            "Generated dynamic operation service " + serviceName + " maps to a non-dynamic YAML step")).from();
        PipelineYamlStep source = owner.stream().filter(candidate -> name.equals(candidate.name())).findFirst()
            .orElseThrow(() -> new IllegalStateException("Dynamic operation service " + serviceName
                + " references unknown callable source '" + name + "'"));
        if (source.callables().isEmpty()) {
            throw new IllegalStateException("Dynamic operation callable source '" + name + "' exposes no capabilities");
        }
        ClassLoader classLoader = PipelineResources.resolveClassLoader();
        ConnectorProviderManifestCatalog catalog = ConnectorProviderManifestLoader.load(classLoader);
        List<DispatchCapability> capabilities = new ArrayList<>();
        source.callables().values().forEach(callable -> capabilities.add(
            capability(config, callable, catalog, classLoader)));
        return OperationDispatchDescriptor.of(serviceName, capabilities);
    }

    private static DispatchCapability capability(
        PipelineYamlConfig config,
        PipelineYamlCallable callable,
        ConnectorProviderManifestCatalog catalog,
        ClassLoader classLoader
    ) {
        PipelineYamlConnectorBinding binding = Optional.ofNullable(config.connectors().get(callable.using()))
            .orElseThrow(() -> new IllegalArgumentException("unknown callable binding '" + callable.using() + "'"));
        ConnectorProviderId provider = ConnectorProviderId.of(binding.provider());
        ConnectorOperationDescriptor operation = catalog.requireOperation(
            provider, binding.version(), callable.operation(), callable.kind(), callable.operationVersion());
        if (ConnectorOperationKind.QUERY.equals(operation.kind())
            && operation.queryCardinality().orElseThrow()
                == org.pipelineframework.connector.QueryOperationCardinality.ONE_TO_MANY) {
            throw new IllegalArgumentException("streaming Query operation cannot be exposed through unary operation dispatch: "
                + callable.using() + "/" + callable.operation());
        }
        var contract = operation.typeContract().orElseThrow(() -> new IllegalArgumentException(
            "callable operation has no normalized type contract: " + callable.using() + "/" + callable.operation()));
        if (!contract.inputType().equals(callable.input())) {
            throw new IllegalArgumentException("callable input contract for " + callable.using() + "/" + callable.operation()
                + " does not match trusted connector metadata: " + contract.inputType());
        }
        String outputType = contract.outputType().orElseThrow(() -> new IllegalArgumentException(
            "callable operation has no output contract: " + callable.using() + "/" + callable.operation()));
        catalog.validateOperationConfiguration(
            provider, binding.version(), callable.operation(), callable.kind(), callable.operationVersion(),
            new ConnectorConfigurationDocument(callable.config()),
            "callable operation '" + callable.alias() + "'");
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            provider, operation.id(), operation.kind(), operation.majorVersion());
        Optional<DispatchCapability.CommandConfiguration> command = Optional.empty();
        if (ConnectorOperationKind.COMMAND.equals(operation.kind())) {
            CommandPolicy policy = policy(callable.policy());
            catalog.validateCommandPolicy(identity, binding.version(), policy);
            command = Optional.of(new DispatchCapability.CommandConfiguration(
                callable.commandIdGenerator().orElseThrow(),
                CommandDuplicatePolicy.fromString(callable.duplicatePolicy()),
                policy));
        }
        return new DispatchCapability(
            new BoundOperationReference(org.pipelineframework.connector.ConnectorBindingName.of(binding.name()), operation.id()),
            identity,
            binding.version(),
            contract.inputType(),
            loadType(config.basePackage(), contract.inputType(), classLoader),
            outputType,
            loadType(config.basePackage(), outputType, classLoader),
            callable.config(),
            ConnectorOperationKind.QUERY.equals(operation.kind())
                ? Optional.of(operation.queryCapabilities().orElse(org.pipelineframework.connector.QueryCapabilities.conservative()))
                : Optional.empty(),
            command);
    }

    private static Class<?> loadType(String basePackage, String type, ClassLoader classLoader) {
        String name = type.contains(".") ? type : basePackage + "." + type;
        try {
            return Class.forName(name, true, classLoader);
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException("Callable canonical type has no generated Java class: " + name, failure);
        }
    }

    private static CommandPolicy policy(Map<String, Object> values) {
        values.keySet().stream().filter(key -> !Set.of(
            "requireRetryRedrive", "requireIdempotency", "requireReconciliation",
            "requiredExecutionPosture", "minimumMachineConfirmation",
            "requireUserConfirmation").contains(key)).sorted().findFirst().ifPresent(key -> {
                throw new IllegalArgumentException("callable command policy has unsupported field '" + key + "'");
            });
        return new CommandPolicy(
            bool(values, "requireRetryRedrive"), bool(values, "requireIdempotency"),
            bool(values, "requireReconciliation"),
            optionalEnum(values, "requiredExecutionPosture", CommandExecutionPosture.class),
            optionalEnum(values, "minimumMachineConfirmation", CommandMachineConfirmation.class),
            bool(values, "requireUserConfirmation"));
    }

    private static boolean bool(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return false;
        }
        if (!(value instanceof Boolean result)) {
            throw new IllegalArgumentException("callable command policy " + key + " must be boolean");
        }
        return result;
    }

    private static <E extends Enum<E>> Optional<E> optionalEnum(Map<String, Object> values, String key, Class<E> type) {
        Object value = values.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (!(value instanceof String token) || token.isBlank()) {
            throw new IllegalArgumentException("callable command policy " + key + " must be a non-blank string");
        }
        try {
            return Optional.of(Enum.valueOf(type, token.trim().toUpperCase(java.util.Locale.ROOT)));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("callable command policy " + key + " has unsupported value '" + token + "'");
        }
    }

    private static Path resolveConfigBase() {
        String explicit = Optional.ofNullable(System.getProperty("pipeline.config"))
            .filter(value -> !value.isBlank())
            .orElseGet(() -> Optional.ofNullable(System.getenv("PIPELINE_CONFIG")).orElse(""));
        if (!explicit.isBlank()) {
            Path candidate = Path.of(explicit);
            if (candidate.isAbsolute()) {
                return candidate.getParent() == null ? candidate : candidate.getParent();
            }
        }
        return Path.of("").toAbsolutePath();
    }

    private static String toServiceName(String name) {
        String[] parts = name.trim().split("[^A-Za-z0-9]+");
        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            if (!part.isEmpty()) {
                result.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
        }
        return result.append("Service").toString();
    }
}
