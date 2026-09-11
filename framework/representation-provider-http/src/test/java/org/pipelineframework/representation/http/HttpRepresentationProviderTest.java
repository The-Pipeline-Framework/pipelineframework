package org.pipelineframework.representation.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
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
    void keepsRuntimeCallbackFieldsOutOfAllAuthoringRoutesAndUsesExistingInboundBindings() {
        var wire = (com.fasterxml.jackson.databind.node.ObjectNode)
            org.pipelineframework.connector.http.HttpPinnedJson.parse(INPUT_SCHEMA);
        ((com.fasterxml.jackson.databind.node.ObjectNode) wire.path("properties"))
            .putObject("callbackUrl").put("type", "string").put("format", "uri");
        ((com.fasterxml.jackson.databind.node.ArrayNode) wire.path("required")).add("callbackUrl");
        var wireSchema = new HttpWireSchema(wire.toString());
        var callback = new org.pipelineframework.connector.http.HttpCallbackPin("job.completed", "job.start", 1,
            new org.pipelineframework.connector.http.HttpCallbackInjectionTarget(
                org.pipelineframework.connector.http.HttpCallbackInjectionTarget.Location.BODY,
                List.of("callbackUrl"), Optional.empty()), "POST", "application/json", new HttpWireSchema(OUTPUT_SCHEMA),
            "http.job.callback", "Callback", HttpSecurityConstraint.none(), 202, true, SOURCE);
        var pin = new HttpOperationPin("job.start", ConnectorOperationKind.COMMAND, 1, "Input", "Output",
            "POST", "/jobs", List.of(), Optional.of(new HttpRequestBodyPin(
                "application/json", Optional.empty(), true, wireSchema)),
            List.of(new HttpResponsePin("202", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                Optional.of("http.job.accepted"), Optional.empty(),
                Optional.of(org.pipelineframework.connector.CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED),
                Optional.of(new HttpWireSchema(OUTPUT_SCHEMA)))),
            HttpSecurityConstraint.none(), wireSchema, "http.job.request", Optional.empty(), List.of(callback), SOURCE);
        var provider = new HttpRepresentationProvider(new HttpOperationCatalog(List.of(pin)));
        var boundary = new OperationBoundaryRequest("proof:job", "http.client", 1, "job.start",
            ConnectorOperationKind.COMMAND.value(), 1, INPUT, OUTPUT);
        var claim = provider.claimOperation(boundary).orElseThrow();
        assertEquals(List.of("/callbackUrl"), claim.request().runtimeSuppliedPaths());
        var direct = new OperationRepresentationRequest(boundary, claim, OperationRepresentationRole.REQUEST,
            INPUT, INPUT_SCHEMA, claim.request(), Optional.empty());
        assertEquals("DIRECT", provider.resolveOperation(direct).orElseThrow().mode());
        var generated = new RepresentationMappingRequest("http.job.request", INPUT, Optional.empty(), Optional.empty(),
            Map.of("fields", Map.of("subject", "subject")));
        assertEquals("GENERATED", provider.resolveOperation(new OperationRepresentationRequest(boundary, claim,
            OperationRepresentationRole.REQUEST, INPUT, INPUT_SCHEMA, claim.request(), Optional.of(generated)))
            .orElseThrow().mode());
        var curated = new RepresentationMappingRequest("http.job.request", INPUT,
            Optional.of("example.JobWire"), Optional.of("example.JobMapper"), Map.of());
        assertEquals("CURATED", provider.resolveOperation(new OperationRepresentationRequest(boundary, claim,
            OperationRepresentationRole.REQUEST, INPUT, INPUT_SCHEMA, claim.request(), Optional.of(curated)))
            .orElseThrow().mode());
        var forged = new RepresentationMappingRequest("http.job.request", INPUT, Optional.empty(), Optional.empty(),
            Map.of("constants", Map.of("callbackUrl", "https://untrusted.test")));
        assertThrows(IllegalArgumentException.class, () -> provider.resolveOperation(new OperationRepresentationRequest(
            boundary, claim, OperationRepresentationRole.REQUEST, INPUT, INPUT_SCHEMA, claim.request(), Optional.of(forged))));
        var inbound = claim.callbacks().getFirst();
        var resolved = provider.resolveOperation(new OperationRepresentationRequest(boundary, claim,
            OperationRepresentationRole.CALLBACK, new CanonicalType("Callback", "example.Callback", CanonicalTypeShape.RECORD),
            OUTPUT_SCHEMA, inbound.wire(), Optional.empty())).orElseThrow();
        var bindings = provider.describeOperationArtifacts(new OperationProviderGenerationRequest(List.of(resolved)))
            .stream().filter(artifact -> artifact.kind() == ArtifactKind.RESOURCE).findFirst().orElseThrow();
        assertTrue(HttpOperationBindingCatalog.read(bindings.content()).find("http.job.callback").isPresent());
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
        Map<String, Object> fieldsFirst = new LinkedHashMap<>();
        fieldsFirst.put("fields", Map.of("subject", "request.subject"));
        fieldsFirst.put("constants", Map.of());
        Map<String, Object> constantsFirst = new LinkedHashMap<>();
        constantsFirst.put("constants", Map.of());
        constantsFirst.put("fields", Map.of("subject", "request.subject"));
        RepresentationMappingRequest authoredFieldsFirst = new RepresentationMappingRequest(
            "http.lookup.request", INPUT, Optional.empty(), Optional.empty(), fieldsFirst);
        RepresentationMappingRequest authoredConstantsFirst = new RepresentationMappingRequest(
            "http.lookup.request", INPUT, Optional.empty(), Optional.empty(), constantsFirst);
        var fieldsFirstRequest = new OperationRepresentationRequest(boundary(), claim,
            OperationRepresentationRole.REQUEST, INPUT, INPUT_SCHEMA, claim.request(), Optional.of(authoredFieldsFirst));
        var constantsFirstRequest = new OperationRepresentationRequest(boundary(), claim,
            OperationRepresentationRole.REQUEST, INPUT, INPUT_SCHEMA, claim.request(),
            Optional.of(authoredConstantsFirst));

        var resolved = provider.resolveOperation(fieldsFirstRequest).orElseThrow();
        var reordered = provider.resolveOperation(constantsFirstRequest).orElseThrow();
        var artifacts = provider.describeOperationArtifacts(new OperationProviderGenerationRequest(List.of(resolved)));

        assertEquals("GENERATED", resolved.mode());
        assertEquals(resolved.mappingFingerprint(), reordered.mappingFingerprint());
        assertEquals(resolved.mapperType(), reordered.mapperType());
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
