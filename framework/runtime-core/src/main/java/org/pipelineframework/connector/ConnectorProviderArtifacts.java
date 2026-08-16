package org.pipelineframework.connector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Generates consumer-side static metadata and direct ServiceLoader registration from providers. */
public final class ConnectorProviderArtifacts {
    public static final String SERVICE_PATH = "META-INF/services/" + "org.pipelineframework.connector.ConnectorProvider";

    private ConnectorProviderArtifacts() {
    }

    public static ConnectorProviderManifest manifest(Collection<? extends ConnectorProvider<?>> providers) {
        Objects.requireNonNull(providers, "providers must not be null");
        List<ConnectorProviderArtifactDescriptor> artifacts = providers.stream()
            .map(provider -> Objects.requireNonNull(provider, "provider must not be null"))
            .sorted(Comparator.comparing(ConnectorProvider::id))
            .map(provider -> new ConnectorProviderArtifactDescriptor(
                ConnectorDescriptors.provider(provider),
                provider.operations().stream().map(ConnectorDescriptors::operation).toList()))
            .toList();
        return new ConnectorProviderManifest(ConnectorProviderManifest.CURRENT_SCHEMA_VERSION, artifacts);
    }

    public static void write(Path classesDirectory, Collection<? extends ConnectorProvider<?>> providers) {
        Objects.requireNonNull(classesDirectory, "classes directory must not be null");
        List<ConnectorProvider<?>> sorted = new ArrayList<>(providers);
        sorted.sort(Comparator.comparing(provider -> provider.getClass().getName()));
        Path manifestPath = classesDirectory.resolve(ConnectorProviderManifestLoader.RESOURCE_PATH);
        Path servicePath = classesDirectory.resolve(SERVICE_PATH);
        try {
            Files.createDirectories(manifestPath.getParent());
            Files.createDirectories(servicePath.getParent());
            Files.writeString(manifestPath, json(manifest(sorted)), StandardCharsets.UTF_8);
            Files.writeString(servicePath,
                sorted.stream().map(provider -> provider.getClass().getName()).reduce("", (left, right) -> left + right + "\n"),
                StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("unable to write connector provider artifacts", exception);
        }
    }

    static String json(ConnectorProviderManifest manifest) {
        StringBuilder json = new StringBuilder("{\"schemaVersion\":")
            .append(manifest.schemaVersion()).append(",\"providers\":[");
        appendJoined(json, manifest.providers(), ConnectorProviderArtifacts::provider);
        return json.append("]}\n").toString();
    }

    private static String provider(ConnectorProviderArtifactDescriptor artifact) {
        ConnectorProviderDescriptor provider = artifact.provider();
        StringBuilder json = new StringBuilder("{\"id\":").append(quote(provider.id().value()))
            .append(",\"version\":{\"major\":").append(provider.version().major())
            .append(",\"minor\":").append(provider.version().minor()).append('}');
        provider.configurationSchema().ifPresent(schema -> json.append(",\"configurationSchema\":").append(schema(schema)));
        provider.executionCapabilities().ifPresent(capabilities -> json.append(",\"executionCapabilities\":{")
            .append("\"executionStyle\":").append(quote(capabilities.executionStyle().name()))
            .append(",\"concurrencyScope\":").append(quote(capabilities.concurrencyScope().name())).append('}'));
        json.append(",\"operations\":[");
        appendJoined(json, artifact.operations(), ConnectorProviderArtifacts::operation);
        return json.append("]}").toString();
    }

    private static String operation(ConnectorOperationDescriptor operation) {
        StringBuilder json = new StringBuilder("{\"id\":").append(quote(operation.id()))
            .append(",\"kind\":").append(quote(operation.kind().value()))
            .append(",\"majorVersion\":").append(operation.majorVersion());
        operation.configurationSchema().ifPresent(schema -> json.append(",\"configurationSchema\":").append(schema(schema)));
        operation.commandCapabilities().ifPresent(capabilities -> json.append(",\"commandCapabilities\":")
            .append(commandCapabilities(capabilities)));
        return json.append('}').toString();
    }

    private static String schema(ConnectorConfigSchemaDescriptor schema) {
        StringBuilder json = new StringBuilder("{\"id\":").append(quote(schema.id()))
            .append(",\"version\":").append(schema.version()).append(",\"fields\":[");
        appendJoined(json, schema.fields(), field -> {
            StringBuilder value = new StringBuilder("{\"name\":").append(quote(field.name()))
                .append(",\"type\":").append(quote(field.type().name()))
                .append(",\"required\":").append(field.required());
            if (!field.enumValues().isEmpty()) {
                value.append(",\"enumValues\":[");
                appendJoined(value, field.enumValues(), ConnectorProviderArtifacts::quote);
                value.append(']');
            }
            return value.append('}').toString();
        });
        return json.append("]}").toString();
    }

    private static String commandCapabilities(CommandCapabilities capabilities) {
        StringBuilder json = new StringBuilder("{")
            .append("\"retryRedriveSupported\":").append(capabilities.retryRedriveSupported())
            .append(",\"providerIdempotencySupported\":").append(capabilities.providerIdempotencySupported())
            .append(",\"reconciliationSupported\":").append(capabilities.reconciliationSupported())
            .append(",\"executionPosture\":").append(quote(capabilities.executionPosture().name()))
            .append(",\"maximumMachineConfirmation\":").append(quote(capabilities.maximumMachineConfirmation().name()))
            .append(",\"userConfirmationSupported\":").append(capabilities.userConfirmationSupported())
            .append(",\"durableReferenceKinds\":[");
        appendJoined(json, capabilities.durableReferenceKinds().stream().sorted().toList(), ConnectorProviderArtifacts::quote);
        return json.append("]}").toString();
    }

    private static <T> void appendJoined(StringBuilder target, Collection<T> values, java.util.function.Function<T, String> mapper) {
        boolean first = true;
        for (T value : values) {
            if (!first) {
                target.append(',');
            }
            first = false;
            target.append(mapper.apply(value));
        }
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
