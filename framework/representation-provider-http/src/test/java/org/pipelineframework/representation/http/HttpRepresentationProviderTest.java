package org.pipelineframework.representation.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.http.HttpOperationBindingCatalog;
import org.pipelineframework.connector.http.HttpOperationCatalog;
import org.pipelineframework.connector.http.HttpOperationPin;
import org.pipelineframework.connector.http.HttpRequestBodyPin;
import org.pipelineframework.connector.http.HttpResponseOutcome;
import org.pipelineframework.connector.http.HttpResponsePin;
import org.pipelineframework.connector.http.HttpSecurityConstraint;
import org.pipelineframework.connector.http.HttpWireSchema;
import org.pipelineframework.representation.spi.ArtifactKind;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.OperationBoundaryClaim;
import org.pipelineframework.representation.spi.OperationBoundaryRequest;
import org.pipelineframework.representation.spi.OperationProviderGenerationRequest;
import org.pipelineframework.representation.spi.OperationRepresentationRequest;
import org.pipelineframework.representation.spi.OperationRepresentationRole;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;

class HttpRepresentationProviderTest {
    private static final String SOURCE = "a".repeat(64);
    private static final CanonicalType INPUT = new CanonicalType("Input", "example.Input", CanonicalTypeShape.RECORD);
    private static final CanonicalType OUTPUT = new CanonicalType("Output", "example.Output", CanonicalTypeShape.RECORD);
    private static final String INPUT_SCHEMA = """
        {"type":"object","additionalProperties":false,"properties":{"subject":{"type":"string"}},"required":["subject"]}
        """;
    private static final String NESTED_INPUT_SCHEMA = """
        {"type":"object","additionalProperties":false,"properties":{"request":{"type":"object","additionalProperties":false,"properties":{"subject":{"type":"string"}},"required":["subject"]}},"required":["request"]}
        """;
    private static final String OUTPUT_SCHEMA = """
        {"type":"object","additionalProperties":false,"properties":{"value":{"type":"string"}},"required":["value"]}
        """;

    @Test
    void claimsOnlyExactPinnedHttpOperationContracts() {
        HttpRepresentationProvider provider = provider(INPUT_SCHEMA);

        OperationBoundaryClaim claim = provider.claimOperation(boundary()).orElseThrow();

        assertEquals("http", claim.providerKey());
        assertEquals("http.lookup.request", claim.request().mappingKey());
        assertEquals(List.of("http.lookup.response"), claim.responses().stream()
            .map(OperationBoundaryClaim.WireBoundary::mappingKey).toList());
        OperationBoundaryRequest wrong = new OperationBoundaryRequest("proof:lookup", "http.client", 1,
            "evidence.lookup", ConnectorOperationKind.QUERY.value(), 1,
            new CanonicalType("Wrong", "example.Wrong", CanonicalTypeShape.RECORD), OUTPUT);
        assertThrows(IllegalStateException.class, () -> provider.claimOperation(wrong));
    }

    @Test
    void resolvesDirectMappingsOnlyForEquivalentShapes() {
        HttpRepresentationProvider provider = provider(INPUT_SCHEMA);
        OperationBoundaryClaim claim = provider.claimOperation(boundary()).orElseThrow();
        var request = new OperationRepresentationRequest(boundary(), claim, OperationRepresentationRole.REQUEST,
            INPUT, INPUT_SCHEMA, claim.request(), Optional.empty());

        var resolved = provider.resolveOperation(request).orElseThrow();

        assertEquals("DIRECT", resolved.mode());
        assertTrue(resolved.mapperType().isEmpty());
    }

    @Test
    void generatesDeterministicMapperAndBindingArtifactsForBoundedOptions() {
        HttpRepresentationProvider provider = provider(NESTED_INPUT_SCHEMA);
        OperationBoundaryClaim claim = provider.claimOperation(boundary()).orElseThrow();
        RepresentationMappingRequest authored = new RepresentationMappingRequest("http.lookup.request", INPUT,
            Optional.empty(), Optional.empty(), Map.of("fields", Map.of("subject", "request.subject")));
        var request = new OperationRepresentationRequest(boundary(), claim, OperationRepresentationRole.REQUEST,
            INPUT, INPUT_SCHEMA, claim.request(), Optional.of(authored));

        var resolved = provider.resolveOperation(request).orElseThrow();
        var repeated = provider.resolveOperation(request).orElseThrow();
        var artifacts = provider.describeOperationArtifacts(new OperationProviderGenerationRequest(List.of(resolved)));

        assertEquals("GENERATED", resolved.mode());
        assertEquals(resolved.mappingFingerprint(), repeated.mappingFingerprint());
        assertEquals(resolved.mapperType(), repeated.mapperType());
        assertTrue(artifacts.stream().anyMatch(artifact -> artifact.kind() == ArtifactKind.JAVA_SOURCE
            && artifact.content().contains("HttpOptionMappingSupport")));
        var resource = artifacts.stream().filter(artifact -> artifact.kind() == ArtifactKind.RESOURCE)
            .findFirst().orElseThrow();
        assertEquals(List.of("http.lookup.request"), HttpOperationBindingCatalog.read(resource.content())
            .bindings().stream().map(binding -> binding.mappingKey()).toList());
    }

    @Test
    void rejectsIncompatibleGeneratedPathsRatherThanGuessing() {
        HttpRepresentationProvider provider = provider(NESTED_INPUT_SCHEMA);
        OperationBoundaryClaim claim = provider.claimOperation(boundary()).orElseThrow();
        RepresentationMappingRequest authored = new RepresentationMappingRequest("http.lookup.request", INPUT,
            Optional.empty(), Optional.empty(), Map.of("fields", Map.of("missing", "request.subject")));
        var request = new OperationRepresentationRequest(boundary(), claim, OperationRepresentationRole.REQUEST,
            INPUT, INPUT_SCHEMA, claim.request(), Optional.of(authored));

        assertThrows(IllegalArgumentException.class, () -> provider.resolveOperation(request));
    }

    @Test
    void preservesAnExplicitCuratedRepresentationAndMapperPair() {
        HttpRepresentationProvider provider = provider(NESTED_INPUT_SCHEMA);
        OperationBoundaryClaim claim = provider.claimOperation(boundary()).orElseThrow();
        RepresentationMappingRequest authored = new RepresentationMappingRequest("http.lookup.request", INPUT,
            Optional.of("example.HttpInput"), Optional.of("example.HttpInputMapper"), Map.of());
        var request = new OperationRepresentationRequest(boundary(), claim, OperationRepresentationRole.REQUEST,
            INPUT, INPUT_SCHEMA, claim.request(), Optional.of(authored));

        var resolved = provider.resolveOperation(request).orElseThrow();

        assertEquals("CURATED", resolved.mode());
        assertEquals(Optional.of("example.HttpInput"), resolved.representationType());
        assertEquals(Optional.of("example.HttpInputMapper"), resolved.mapperType());
    }

    @Test
    void ignoresSchemaAnnotationsAtEveryDepthForDirectMappings() {
        String annotated = """
            {
              "title": "Input",
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "subject": { "type": "string", "description": "A subject", "examples": [ "one" ] }
              },
              "required": [ "subject" ]
            }
            """;
        HttpRepresentationProvider provider = provider(INPUT_SCHEMA);
        OperationBoundaryClaim claim = provider.claimOperation(boundary()).orElseThrow();
        var request = new OperationRepresentationRequest(boundary(), claim, OperationRepresentationRole.REQUEST,
            INPUT, annotated, claim.request(), Optional.empty());

        assertEquals("DIRECT", provider.resolveOperation(request).orElseThrow().mode());
    }

    private static HttpRepresentationProvider provider(String requestSchema) {
        return new HttpRepresentationProvider(new HttpOperationCatalog(List.of(new HttpOperationPin(
            "evidence.lookup", ConnectorOperationKind.QUERY, 1, "Input", "Output", "POST", "/evidence",
            List.of(), Optional.of(new HttpRequestBodyPin("application/json", Optional.empty(), true,
                new HttpWireSchema(requestSchema))),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.RESULT,
                Optional.of("http.lookup.response"), Optional.empty(), Optional.empty(),
                Optional.of(new HttpWireSchema(OUTPUT_SCHEMA)))),
            HttpSecurityConstraint.none(), new HttpWireSchema(requestSchema), "http.lookup.request",
            Optional.empty(), SOURCE))));
    }

    private static OperationBoundaryRequest boundary() {
        return new OperationBoundaryRequest("proof:lookup", "http.client", 1, "evidence.lookup",
            ConnectorOperationKind.QUERY.value(), 1, INPUT, OUTPUT);
    }
}
