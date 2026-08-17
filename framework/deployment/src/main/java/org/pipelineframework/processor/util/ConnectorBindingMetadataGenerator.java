package org.pipelineframework.processor.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlOperationSelection;
import org.pipelineframework.connector.ConnectorConfigSchemaDescriptor;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.processor.PipelineCompilationContext;

/**
 * Emits sanitized immutable metadata for configured connector bindings and their step references.
 */
public final class ConnectorBindingMetadataGenerator {
    public static final String RESOURCE_PATH = "META-INF/pipeline/connector-bindings.json";
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private final ProcessingEnvironment processingEnv;

    public ConnectorBindingMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = Objects.requireNonNull(processingEnv, "processing environment must not be null");
    }

    public void writeMetadata(PipelineCompilationContext context) throws IOException {
        Optional<Path> configPath = resolvePipelineConfigPath(context);
        if (configPath.isEmpty()) {
            return;
        }
        PipelineYamlConfig config = new PipelineYamlConfigLoader(processingEnv.getOptions()::get, System::getenv)
            .load(configPath.orElseThrow());
        if (config.connectors().isEmpty()) {
            return;
        }
        ConnectorProviderManifestCatalog catalog = ConnectorProviderManifestLoader.load(metadataClassLoader());
        List<BindingMetadata> bindings = config.connectors().values().stream()
            .sorted(Comparator.comparing(PipelineYamlConnectorBinding::name))
            .map(binding -> metadata(binding, config, catalog))
            .toList();
        var resource = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_PATH);
        try (var writer = resource.openWriter()) {
            writer.write(GSON.toJson(new Metadata(2, bindings)));
        }
    }

    private static BindingMetadata metadata(
        PipelineYamlConnectorBinding binding,
        PipelineYamlConfig config,
        ConnectorProviderManifestCatalog catalog
    ) {
        ConnectorProviderId providerId = ConnectorProviderId.of(binding.provider());
        ConnectorProviderArtifactDescriptor provider = catalog.requireProvider(providerId, binding.version());
        ConnectorConfigurationDocument document = new ConnectorConfigurationDocument(binding.config());
        catalog.validateProviderConfiguration(
            providerId,
            binding.version(),
            document,
            "connector binding '" + binding.name() + "' provider " + providerId.value());
        Map<String, Object> configuration = sanitizedConfiguration(provider, document);
        List<OperationReference> operations = config.stepDefinitions().values().stream().flatMap(List::stream)
            .filter(step -> step.operationSelection().isPresent())
            .filter(step -> binding.name().equals(step.operationSelection().orElseThrow().using()))
            .map(step -> operationReference(step.name(), step.kind(), step.operationSelection().orElseThrow()))
            .sorted(Comparator.comparing(OperationReference::step))
            .toList();
        List<CallableReference> callables = config.stepDefinitions().values().stream().flatMap(List::stream)
            .flatMap(step -> step.callables().values().stream()
                .filter(callable -> binding.name().equals(callable.using()))
                .map(callable -> callableReference(step.name(), callable, providerId, binding.version(), catalog)))
            .sorted(Comparator.comparing(CallableReference::step).thenComparing(CallableReference::alias))
            .toList();
        return new BindingMetadata(binding.name(), binding.provider(), binding.version(), configuration, operations, callables);
    }

    private static CallableReference callableReference(
        String step,
        org.pipelineframework.config.pipeline.PipelineYamlCallable callable,
        ConnectorProviderId providerId,
        int providerVersion,
        ConnectorProviderManifestCatalog catalog
    ) {
        var operation = catalog.requireOperation(
            providerId, providerVersion, callable.operation(), callable.kind(), callable.operationVersion());
        var contract = operation.typeContract().orElseThrow(() -> new IllegalArgumentException(
            "callable operation has no normalized type contract: " + callable.using() + "/" + callable.operation()));
        if (!contract.inputType().equals(callable.input())) {
            throw new IllegalArgumentException("callable input contract for " + callable.using() + "/" + callable.operation()
                + " does not match trusted connector metadata: " + contract.inputType());
        }
        return new CallableReference(
            step, callable.alias(), operation.kind().value(), operation.id(), operation.majorVersion(),
            contract.inputType(), contract.outputType().orElseThrow(() -> new IllegalArgumentException(
                "callable operation has no output contract: " + callable.using() + "/" + callable.operation())));
    }

    private static Map<String, Object> sanitizedConfiguration(
        ConnectorProviderArtifactDescriptor provider,
        ConnectorConfigurationDocument document
    ) {
        Optional<ConnectorConfigSchemaDescriptor> schema = provider.provider().configurationSchema();
        if (schema.isEmpty()) {
            return Map.of();
        }
        ConnectorConfigurationSnapshot snapshot = ConnectorConfigurationSnapshot.from(
            schema.orElseThrow(), document, false);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schemaId", snapshot.schemaId());
        result.put("schemaVersion", snapshot.schemaVersion());
        result.put("digest", snapshot.digest());
        return Collections.unmodifiableMap(result);
    }

    private static OperationReference operationReference(
        String step,
        String kind,
        PipelineYamlOperationSelection selection
    ) {
        return new OperationReference(step, kind.toLowerCase(java.util.Locale.ROOT), selection.operation(), selection.operationVersion());
    }

    private Optional<Path> resolvePipelineConfigPath(PipelineCompilationContext context) {
        String explicit = processingEnv.getOptions().get("pipeline.config");
        if (explicit != null && !explicit.isBlank()) {
            Path path = Path.of(explicit.trim());
            if (!path.isAbsolute()) {
                if (context.getModuleDir() == null) {
                    return Optional.empty();
                }
                path = context.getModuleDir().resolve(path).normalize();
            }
            if (Files.exists(path)) {
                return Optional.of(path);
            }
        }
        return context.getModuleDir() == null
            ? Optional.empty()
            : new PipelineYamlConfigLocator().locate(context.getModuleDir());
    }

    private static ClassLoader metadataClassLoader() {
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        return context == null ? ConnectorBindingMetadataGenerator.class.getClassLoader() : context;
    }

    private record Metadata(int schemaVersion, List<BindingMetadata> bindings) {
    }

    private record BindingMetadata(
        String name,
        String provider,
        int providerVersion,
        Map<String, Object> configuration,
        List<OperationReference> operations,
        List<CallableReference> callables
    ) {
    }

    private record OperationReference(String step, String kind, String operation, int operationVersion) {
    }

    private record CallableReference(
        String step,
        String alias,
        String kind,
        String operation,
        int operationVersion,
        String input,
        String output
    ) {
    }
}
