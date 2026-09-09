package org.pipelineframework.connector.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectorOperationKind;

class HttpRequestProjectorTest {
    private static final String SOURCE = "1".repeat(64);
    private static final HttpWireSchema OUTPUT = new HttpWireSchema("{\"type\":\"object\"}");

    @Test
    void serializesSimpleLabelAndMatrixPathStylesWithoutEscapingStructuralDelimiters() {
        HttpWireSchema input = new HttpWireSchema("""
            {"type":"object","additionalProperties":false,"required":["simple","label","matrix"],"properties":{
              "simple":{"type":"array","items":{"type":"string"}},
              "label":{"type":"object","additionalProperties":false,"properties":{"first":{"type":"string"},"role":{"type":"string"}}},
              "matrix":{"type":"array","items":{"type":"integer"}}
            }}
            """);
        HttpOperationPin pin = pin("/items/{simple}/{label}/{matrix}", input, List.of(
            parameter("simple", HttpParameterLocation.PATH, HttpParameterStyle.SIMPLE, false,
                new HttpWireSchema("{\"type\":\"array\",\"items\":{\"type\":\"string\"}}")),
            parameter("label", HttpParameterLocation.PATH, HttpParameterStyle.LABEL, true,
                new HttpWireSchema("{\"type\":\"object\"}")),
            parameter("matrix", HttpParameterLocation.PATH, HttpParameterStyle.MATRIX, true,
                new HttpWireSchema("{\"type\":\"array\",\"items\":{\"type\":\"integer\"}}"))));

        var request = HttpRequestProjector.project(pin, HttpPinnedJson.parse("""
            {"simple":["blue","black"],"label":{"role":"admin","first":"Alex"},"matrix":[3,4]}
            """), connection(), HttpAuthorizationMaterial.none(), configuration(), Optional.empty());

        assertEquals("/api/items/blue,black/.first=Alex.role=admin/;matrix=3;matrix=4",
            request.uri().getRawPath());
    }

    @Test
    void serializesFormDelimitedAndDeepObjectQueryStyles() {
        HttpWireSchema input = new HttpWireSchema("""
            {"type":"object","required":["tag","space","pipe","filter"],"properties":{
              "tag":{"type":"array","items":{"type":"string"}},
              "space":{"type":"array","items":{"type":"string"}},
              "pipe":{"type":"array","items":{"type":"string"}},
              "filter":{"type":"object","properties":{"first":{"type":"string"},"role":{"type":"string"}}}
            }}
            """);
        HttpOperationPin pin = pin("/search", input, List.of(
            parameter("tag", HttpParameterLocation.QUERY, HttpParameterStyle.FORM, true,
                new HttpWireSchema("{\"type\":\"array\"}")),
            parameter("space", HttpParameterLocation.QUERY, HttpParameterStyle.SPACE_DELIMITED, false,
                new HttpWireSchema("{\"type\":\"array\"}")),
            parameter("pipe", HttpParameterLocation.QUERY, HttpParameterStyle.PIPE_DELIMITED, false,
                new HttpWireSchema("{\"type\":\"array\"}")),
            parameter("filter", HttpParameterLocation.QUERY, HttpParameterStyle.DEEP_OBJECT, true,
                new HttpWireSchema("{\"type\":\"object\"}"))));

        var request = HttpRequestProjector.project(pin, HttpPinnedJson.parse("""
            {"tag":["a","b"],"space":["a","b"],"pipe":["a","b"],"filter":{"role":"admin","first":"Alex"}}
            """), connection(), HttpAuthorizationMaterial.none(), configuration(), Optional.empty());

        assertEquals("tag=a&tag=b&space=a%20b&pipe=a%7Cb&filter%5Bfirst%5D=Alex&filter%5Brole%5D=admin",
            request.uri().getRawQuery());
    }

    @Test
    void rejectsPathValuesThatWouldNormalizeOutsideThePinnedTemplate() {
        HttpWireSchema input = new HttpWireSchema("""
            {"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}
            """);
        HttpOperationPin pin = pin("/items/{id}", input, List.of(
            parameter("id", HttpParameterLocation.PATH, HttpParameterStyle.SIMPLE, false,
                new HttpWireSchema("{\"type\":\"string\"}"))));

        assertThrows(IllegalArgumentException.class, () -> HttpRequestProjector.project(pin,
            HttpPinnedJson.parse("{\"id\":\"..\"}"), connection(), HttpAuthorizationMaterial.none(),
            configuration(), Optional.empty()));
    }

    @Test
    void nonServerUrisNeverShareAnHttpOrigin() {
        assertFalse(HttpRequestProjector.sameOrigin(
            URI.create("https://example.test"), URI.create("mailto:someone@example.test")));
    }

    private static HttpParameterPin parameter(
        String name,
        HttpParameterLocation location,
        HttpParameterStyle style,
        boolean explode,
        HttpWireSchema schema
    ) {
        return new HttpParameterPin(name, location, name, style, explode, true, false, schema);
    }

    private static HttpOperationPin pin(String path, HttpWireSchema input, List<HttpParameterPin> parameters) {
        return new HttpOperationPin("serialization.proof", ConnectorOperationKind.QUERY, 1,
            "Input", "Output", "GET", path, parameters, Optional.empty(),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.RESULT,
                Optional.of("http.serialization.response"), Optional.empty(), Optional.empty(), Optional.of(OUTPUT))),
            HttpSecurityConstraint.none(), input, "http.serialization.request", Optional.empty(), SOURCE);
    }

    private static HttpClientConnection connection() {
        return new HttpClientConnection(HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
            URI.create("https://example.test/api"), Set.of(), HttpAuthorizationProvider.none());
    }

    private static HttpProviderConfiguration configuration() {
        return new HttpProviderConfiguration(new ConnectionRef("test"), Optional.empty(), Optional.empty(),
            Optional.empty());
    }
}
