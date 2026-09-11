package org.pipelineframework.representation.http;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.http.HttpOperationBindingCatalog;
import org.pipelineframework.connector.http.HttpOperationCatalog;
import org.pipelineframework.connector.http.HttpOperationPin;
import org.pipelineframework.connector.http.HttpOperationRepresentationBinding;
import org.pipelineframework.connector.http.HttpPinnedJson;
import org.pipelineframework.connector.http.HttpRepresentationMappingOptions;
import org.pipelineframework.connector.http.HttpRepresentationMode;
import org.pipelineframework.representation.spi.ArtifactDescription;
import org.pipelineframework.representation.spi.ArtifactKind;
import org.pipelineframework.representation.spi.ArtifactPhase;
import org.pipelineframework.representation.spi.OperationBoundaryClaim;
import org.pipelineframework.representation.spi.OperationBoundaryRequest;
import org.pipelineframework.representation.spi.OperationProviderGenerationRequest;
import org.pipelineframework.representation.spi.OperationRepresentationRequest;
import org.pipelineframework.representation.spi.ProviderMetadata;
import org.pipelineframework.representation.spi.ProviderSchemaFragment;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;
import org.pipelineframework.representation.spi.RepresentationProvider;
import org.pipelineframework.representation.spi.ResolvedOperationRepresentation;

/** Resolves deterministic mappings for operations pinned by the generic HTTP Connector. */
public final class HttpRepresentationProvider implements RepresentationProvider {
    public static final String KEY = "http";
    private static final String CONNECTOR = "http.client";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Map<String, HttpOperationPin> pins;

    public HttpRepresentationProvider() {
        this(HttpOperationCatalog.load(ConnectorProviderManifestLoader.metadataClassLoader(
            HttpRepresentationProvider.class)));
    }

    HttpRepresentationProvider(HttpOperationCatalog catalogue) {
        Map<String, HttpOperationPin> indexed = new LinkedHashMap<>();
        catalogue.operations().forEach(pin -> indexed.put(pin.identity(), pin));
        pins = Map.copyOf(indexed);
    }

    @Override
    public ProviderMetadata metadata() {
        return new ProviderMetadata(KEY, Set.of(), Set.of(
            "connector-operation", "direct", "generated-options", "explicit-mapper"));
    }

    @Override
    public boolean supportsOperationProvider(String connectorProviderId, int connectorProviderMajorVersion) {
        return CONNECTOR.equals(connectorProviderId) && connectorProviderMajorVersion == 1;
    }

    @Override
    public Optional<OperationBoundaryClaim> claimOperation(OperationBoundaryRequest boundary) {
        if (!CONNECTOR.equals(boundary.connectorProviderId())) return Optional.empty();
        if (boundary.connectorProviderMajorVersion() != 1) {
            throw new IllegalStateException("HTTP representation provider requires http.client major version 1");
        }
        String identity = boundary.operationKind() + ":" + boundary.operationId() + ":" + boundary.operationMajorVersion();
        HttpOperationPin pin = Optional.ofNullable(pins.get(identity)).orElseThrow(() ->
            new IllegalStateException("selected http.client operation has no immutable HTTP pin: " + identity));
        requireCanonical(pin.inputType(), boundary.inputType().name(), boundary.inputType().targetTypeName(), "input");
        requireCanonical(pin.outputType(), boundary.outputType().name(), boundary.outputType().targetTypeName(), "output");
        OperationBoundaryClaim.WireBoundary request = requestBoundary(pin);
        List<OperationBoundaryClaim.WireBoundary> responses = pin.responses().stream()
            .filter(response -> response.mappingKey().isPresent())
            .map(response -> new OperationBoundaryClaim.WireBoundary(response.mappingKey().orElseThrow(),
                response.schema().orElseThrow().canonicalJson(), response.schema().orElseThrow().sha256()))
            .distinct().toList();
        List<OperationBoundaryClaim.CallbackBoundary> callbacks = pin.callbacks().stream().map(callback ->
            new OperationBoundaryClaim.CallbackBoundary(callback.id(), callback.inputType(),
                new OperationBoundaryClaim.WireBoundary(callback.requestMappingKey(),
                    callback.requestSchema().canonicalJson(), callback.requestSchema().sha256()))).toList();
        return Optional.of(new OperationBoundaryClaim(KEY, request, responses, callbacks));
    }

    @Override
    public Optional<ResolvedOperationRepresentation> resolveOperation(OperationRepresentationRequest request) {
        if (!KEY.equals(request.claim().providerKey())) return Optional.empty();
        String authorableWireSchema = new org.pipelineframework.connector.http.HttpWireSchema(
            request.wireBoundary().schemaJson()).withoutRuntimeSuppliedPaths(request.runtimeSuppliedPaths()).canonicalJson();
        Optional<RepresentationMappingRequest> authored = request.authoredMapping();
        if (authored.isEmpty() || direct(authored.orElseThrow())) {
            if (!HttpSchemaCompatibility.equivalent(request.canonicalSchemaJson(),
                authorableWireSchema)) {
                throw new IllegalStateException("HTTP operation mapping '" + request.wireBoundary().mappingKey()
                    + "' is not direct; add deterministic options or an explicit representation type and Mapper");
            }
            return Optional.of(resolved(request, HttpRepresentationMode.DIRECT, Optional.empty(), Optional.empty(),
                Map.of()));
        }
        RepresentationMappingRequest mapping = authored.orElseThrow();
        if (mapping.representationType().isPresent() != mapping.mapperType().isPresent()) {
            throw new IllegalStateException("HTTP operation mapping '" + mapping.key()
                + "' requires both representation type and mapper");
        }
        if (mapping.mapperType().isPresent()) {
            if (!mapping.options().isEmpty()) {
                throw new IllegalStateException("curated HTTP operation mapping '" + mapping.key()
                    + "' cannot also declare generated mapping options");
            }
            return Optional.of(resolved(request, HttpRepresentationMode.CURATED,
                mapping.representationType(), mapping.mapperType(), Map.of()));
        }
        HttpRepresentationMappingOptions options = HttpRepresentationMappingOptions.from(mapping.options());
        HttpSchemaCompatibility.validate(request.canonicalSchemaJson(), authorableWireSchema, options);
        String fingerprint = fingerprint(request, HttpRepresentationMode.GENERATED, mapping.options());
        String mapperType = "org.pipelineframework.generated.http.HttpMapping_"
            + sanitize(mapping.key()) + "_" + fingerprint.substring(0, 12);
        return Optional.of(new ResolvedOperationRepresentation(KEY, request.boundary().boundaryIdentity(), request.role(),
            mapping.key(), request.canonicalType(), HttpRepresentationMode.GENERATED.name(),
            Optional.of("com.fasterxml.jackson.databind.JsonNode"), Optional.of(mapperType), fingerprint,
            mapping.options(), canonicalFingerprint(request)));
    }

    @Override
    public List<ArtifactDescription> describeOperationArtifacts(OperationProviderGenerationRequest request) {
        List<ResolvedOperationRepresentation> values = request.representations().stream()
            .filter(value -> KEY.equals(value.providerKey()))
            .sorted(Comparator.comparing(ResolvedOperationRepresentation::mappingKey)).toList();
        List<ArtifactDescription> artifacts = new ArrayList<>();
        values.stream().filter(value -> HttpRepresentationMode.GENERATED.name().equals(value.mode()))
            .forEach(value -> artifacts.add(mapperArtifact(value)));
        List<HttpOperationRepresentationBinding> bindings = values.stream().map(value ->
            new HttpOperationRepresentationBinding(value.mappingKey(), HttpRepresentationMode.valueOf(value.mode()),
                value.representationType(), value.mapperType(), value.mappingFingerprint())).toList();
        artifacts.add(new ArtifactDescription(KEY, ArtifactPhase.RESOURCE, ArtifactKind.RESOURCE,
            HttpOperationBindingCatalog.RESOURCE_PATH, new HttpOperationBindingCatalog(bindings).json(), 0));
        return List.copyOf(artifacts);
    }

    @Override
    public ProviderSchemaFragment schema() {
        return new ProviderSchemaFragment(KEY, Optional.empty(), Optional.of("""
            {"type":"object","additionalProperties":false,"properties":{
              "type":{"type":"string"},"mapper":{"type":"string"},
                "options":{"type":"object","additionalProperties":false,
                "properties":{"fields":{"type":"object"},"constants":{"type":"object"},
                  "enums":{"type":"object"},"jsonObjects":{"type":"object"},"collections":{"type":"array"},
                  "discriminator":{"type":"object"}}}}}
            """.strip()), Optional.of(
                "HTTP operation mappings are direct, bounded deterministic options, or an exact curated Mapper."));
    }

    private static OperationBoundaryClaim.WireBoundary requestBoundary(HttpOperationPin pin) {
        return new OperationBoundaryClaim.WireBoundary(pin.requestMappingKey(),
            pin.requestSchema().canonicalJson(), pin.requestSchema().sha256(), pin.runtimeSuppliedPaths());
    }

    private static ResolvedOperationRepresentation resolved(
        OperationRepresentationRequest request,
        HttpRepresentationMode mode,
        Optional<String> representationType,
        Optional<String> mapperType,
        Map<String, Object> options
    ) {
        return new ResolvedOperationRepresentation(KEY, request.boundary().boundaryIdentity(), request.role(),
            request.wireBoundary().mappingKey(), request.canonicalType(), mode.name(), representationType, mapperType,
            fingerprint(request, mode, options), options, canonicalFingerprint(request));
    }

    private static Optional<String> canonicalFingerprint(OperationRepresentationRequest request) {
        if (request.runtimeSuppliedPaths().isEmpty()
            && request.role() != org.pipelineframework.representation.spi.OperationRepresentationRole.CALLBACK) {
            return Optional.empty();
        }
        return Optional.of(HttpPinnedJson.sha256(HttpPinnedJson.canonicalize(
            HttpPinnedJson.parse(request.canonicalSchemaJson()))));
    }

    private static String fingerprint(
        OperationRepresentationRequest request,
        HttpRepresentationMode mode,
        Map<String, Object> options
    ) {
        var node = JSON.createObjectNode();
        node.put("mappingKey", request.wireBoundary().mappingKey());
        node.put("role", request.role().name());
        node.put("canonicalType", request.canonicalType().name());
        node.set("canonicalSchema", HttpPinnedJson.parse(request.canonicalSchemaJson()));
        node.put("wireSchemaFingerprint", request.wireBoundary().schemaFingerprint());
        node.put("mode", mode.name());
        node.set("options", JSON.valueToTree(options));
        if (!request.runtimeSuppliedPaths().isEmpty()) {
            node.set("runtimeSuppliedPaths", JSON.valueToTree(request.runtimeSuppliedPaths()));
        }
        request.authoredMapping().flatMap(RepresentationMappingRequest::representationType)
            .ifPresent(value -> node.put("representationType", value));
        request.authoredMapping().flatMap(RepresentationMappingRequest::mapperType)
            .ifPresent(value -> node.put("mapperType", value));
        return HttpPinnedJson.sha256(HttpPinnedJson.canonicalize(node));
    }

    private static boolean direct(RepresentationMappingRequest mapping) {
        return mapping.representationType().isEmpty() && mapping.mapperType().isEmpty() && mapping.options().isEmpty();
    }

    private static void requireCanonical(String expected, String identity, String javaType, String role) {
        if (!expected.equals(identity) && !expected.equals(javaType)) {
            throw new IllegalStateException("pinned HTTP " + role + " canonical identity '" + expected
                + "' disagrees with selected operation boundary '" + identity + "' (" + javaType + ")");
        }
    }

    private static String sanitize(String value) {
        String result = value.replaceAll("[^A-Za-z0-9]+", "_");
        return Character.isJavaIdentifierStart(result.charAt(0)) ? result : "M_" + result;
    }

    private static ArtifactDescription mapperArtifact(ResolvedOperationRepresentation value) {
        String mapper = value.mapperType().orElseThrow();
        int separator = mapper.lastIndexOf('.');
        String packageName = mapper.substring(0, separator);
        String simpleName = mapper.substring(separator + 1);
        String options = HttpPinnedJson.canonicalize(JSON.valueToTree(value.generationConfiguration()));
        String escaped = options.replace("\\", "\\\\").replace("\"", "\\\"");
        String source = """
            package %s;

            public final class %s implements org.pipelineframework.mapper.Mapper<%s, com.fasterxml.jackson.databind.JsonNode> {
                private static final com.fasterxml.jackson.databind.ObjectMapper JSON = new com.fasterxml.jackson.databind.ObjectMapper();
                private static final org.pipelineframework.connector.http.HttpRepresentationMappingOptions OPTIONS = options();

                @Override
                public %s fromExternal(com.fasterxml.jackson.databind.JsonNode external) {
                    return JSON.convertValue(org.pipelineframework.connector.http.HttpOptionMappingSupport.fromWire(external, OPTIONS), %s.class);
                }

                @Override
                public com.fasterxml.jackson.databind.JsonNode toExternal(%s domain) {
                    return org.pipelineframework.connector.http.HttpOptionMappingSupport.toWire(JSON.valueToTree(domain), OPTIONS);
                }

                @SuppressWarnings("unchecked")
                private static org.pipelineframework.connector.http.HttpRepresentationMappingOptions options() {
                    try {
                        java.util.Map<String, Object> value = JSON.readValue("%s", java.util.Map.class);
                        return org.pipelineframework.connector.http.HttpRepresentationMappingOptions.from(value);
                    } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
                        throw new IllegalStateException("invalid generated HTTP representation options", failure);
                    }
                }
            }
            """.formatted(packageName, simpleName, value.canonicalType().targetTypeName(),
                value.canonicalType().targetTypeName(), value.canonicalType().targetTypeName(),
                value.canonicalType().targetTypeName(), escaped);
        return new ArtifactDescription(KEY, ArtifactPhase.SOURCE, ArtifactKind.JAVA_SOURCE,
            mapper.replace('.', '/') + ".java", source, 0);
    }
}
