package org.pipelineframework.connector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pipelineframework.protocol.ProtocolTypeDescriptor;

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

    public static String json(ConnectorProviderManifest manifest) {
        Objects.requireNonNull(manifest, "connector provider manifest must not be null");
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
        json.append(",\"operations\":[");
        appendJoined(json, artifact.operations(), ConnectorProviderArtifacts::operation);
        json.append(']');
        if (!artifact.protocolTypes().isEmpty()) {
            json.append(",\"protocolTypes\":[");
            appendJoined(json, artifact.protocolTypes(), ConnectorProviderArtifacts::protocolType);
            json.append(']');
        }
        return json.append('}').toString();
    }

    private static String protocolType(ProtocolTypeDescriptor descriptor) {
        var definition = descriptor.definition();
        StringBuilder json = new StringBuilder("{\"name\":").append(quote(definition.name()));
        if (definition instanceof org.pipelineframework.config.template.PipelineTemplateTypeDefinition.RecordType record) {
            json.append(",\"fields\":[");
            appendJoined(json, record.fields(), ConnectorProviderArtifacts::protocolField);
            return json.append("]}").toString();
        }
        if (definition instanceof org.pipelineframework.config.template.PipelineTemplateTypeDefinition.WrapperType wrapper) {
            json.append(",\"wraps\":").append(quote(wrapper.wraps().name()));
            appendConstraints(json, wrapper.constraints());
            return json.append('}').toString();
        }
        if (definition instanceof org.pipelineframework.config.template.PipelineTemplateTypeDefinition.AliasType alias) {
            return json.append(",\"alias\":").append(quote(protocolReference(alias.target()))).append('}').toString();
        }
        var union = (org.pipelineframework.config.template.PipelineTemplateTypeDefinition.UnionType) definition;
        json.append(",\"variants\":{");
        appendJoined(json, union.variants().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList(), entry ->
            quote(entry.getKey()) + ":" + quote(protocolReference(entry.getValue().payload())));
        return json.append("}}").toString();
    }

    private static String protocolField(
        org.pipelineframework.config.template.PipelineTemplateTypeDefinition.Field field
    ) {
        StringBuilder json = new StringBuilder("{\"name\":").append(quote(field.name()))
            .append(",\"type\":").append(quote(protocolReference(field.type())));
        if (field.repeated()) {
            json.append(",\"repeated\":true");
        }
        if (field.presence()
            != org.pipelineframework.config.template.PipelineFieldPresence.REQUIRED) {
            json.append(",\"presence\":").append(quote(field.presence().name()));
        }
        if (field.nullability()
            != org.pipelineframework.config.template.PipelineFieldNullability.NON_NULL) {
            json.append(",\"nullability\":").append(quote(field.nullability().name()));
        }
        return json.append('}').toString();
    }

    private static String protocolReference(
        org.pipelineframework.config.template.PipelineTemplateTypeReference reference
    ) {
        if (reference instanceof org.pipelineframework.config.template.PipelineTemplateTypeReference.Scalar scalar) {
            return scalar.name();
        }
        if (reference instanceof org.pipelineframework.config.template.PipelineTemplateTypeReference.Contributed contributed) {
            return contributed.name();
        }
        throw new IllegalArgumentException("provider protocol type contains a non-portable reference: " + reference);
    }

    private static void appendConstraints(
        StringBuilder json,
        org.pipelineframework.config.template.PipelineTemplateWrapperConstraints constraints
    ) {
        constraints.minLength().ifPresent(value -> json.append(",\"minLength\":").append(value));
        constraints.maxLength().ifPresent(value -> json.append(",\"maxLength\":").append(value));
        constraints.pattern().ifPresent(value -> json.append(",\"pattern\":").append(quote(value)));
        constraints.format().ifPresent(value -> json.append(",\"format\":")
            .append(quote(value.name().toLowerCase(java.util.Locale.ROOT))));
        constraints.minimum().ifPresent(value -> json.append(",\"minimum\":").append(value.toPlainString()));
        constraints.minimumExclusive().ifPresent(value -> json.append(",\"minimumExclusive\":").append(value.toPlainString()));
        constraints.maximum().ifPresent(value -> json.append(",\"maximum\":").append(value.toPlainString()));
        constraints.maximumExclusive().ifPresent(value -> json.append(",\"maximumExclusive\":").append(value.toPlainString()));
    }

    private static String operation(ConnectorOperationDescriptor operation) {
        StringBuilder json = new StringBuilder("{\"id\":").append(quote(operation.id()))
            .append(",\"kind\":").append(quote(operation.kind().value()))
            .append(",\"majorVersion\":").append(operation.majorVersion());
        operation.configurationSchema().ifPresent(schema -> json.append(",\"configurationSchema\":").append(schema(schema)));
        operation.commandCapabilities().ifPresent(capabilities -> json.append(",\"commandCapabilities\":")
            .append(commandCapabilities(capabilities)));
        operation.queryCapabilities().ifPresent(capabilities -> json.append(",\"queryCapabilities\":")
            .append(queryCapabilities(capabilities)));
        operation.queryCardinality().ifPresent(cardinality -> json.append(",\"queryCardinality\":")
            .append(quote(cardinality.name())));
        operation.typeContract().ifPresent(contract -> {
            json.append(",\"typeContract\":{\"input\":").append(quote(contract.inputType()));
            contract.outputType().ifPresent(output -> json.append(",\"output\":").append(quote(output)));
            json.append('}');
        });
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

    private static String queryCapabilities(QueryCapabilities capabilities) {
        StringBuilder json = new StringBuilder("{\"cacheability\":")
            .append(quote(capabilities.cacheability().name()));
        capabilities.maximumCacheAge().ifPresent(value ->
            json.append(",\"maximumCacheAge\":").append(quote(value.toString())));
        capabilities.maximumNegativeCacheTtl().ifPresent(value ->
            json.append(",\"maximumNegativeCacheTtl\":").append(quote(value.toString())));
        return json.append('}').toString();
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
        StringBuilder escaped = new StringBuilder("\"");
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '\"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character <= 0x1f) {
                        escaped.append(String.format("\\u%04x", (int) character));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.append('\"').toString();
    }
}
