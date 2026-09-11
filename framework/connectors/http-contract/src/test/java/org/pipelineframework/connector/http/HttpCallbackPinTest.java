package org.pipelineframework.connector.http;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.ConnectorCallbackContext;
import org.pipelineframework.connector.ConnectorOperationKind;
import static org.junit.jupiter.api.Assertions.*;

class HttpCallbackPinTest {
    private static final String SOURCE = "a".repeat(64);
    private static final HttpWireSchema REQUEST = new HttpWireSchema("""
        {"type":"object","properties":{"value":{"type":"string"},"callbackUrl":{"type":"string","format":"uri"}},
         "required":["value","callbackUrl"],"additionalProperties":false}
        """);
    private static final HttpWireSchema PAYLOAD = new HttpWireSchema("""
        {"type":"object","properties":{"status":{"type":"string"}},"required":["status"]}
        """);

    @Test
    void roundTripsCallbacksAndRejectsSchemaOneAndTampering() {
        HttpOperationPin pin = operation(List.of(callback(202)));
        String json = new HttpOperationCatalog(List.of(pin)).json();
        assertEquals(List.of(pin), HttpOperationCatalog.read(json).operations());
        assertEquals(List.of("/callbackUrl"), pin.runtimeSuppliedPaths());
        assertThrows(IllegalArgumentException.class, () -> HttpOperationCatalog.read(
            json.replace("\"schemaVersion\":2", "\"schemaVersion\":1")));
        assertThrows(IllegalArgumentException.class, () -> HttpOperationCatalog.read(
            json.replace("\"acknowledgementStatus\":202", "\"acknowledgementStatus\":204")));
        assertNotEquals(pin.operationFingerprint(), operation(List.of(callback(204))).operationFingerprint());
        assertEquals("JobCallback", callback(202).descriptor().typeContract().inputType());
    }

    @Test
    void keepsSynchronousPinsReadableWithoutAddingCallbackFingerprintMaterial() {
        HttpOperationPin pin = operation(List.of());
        String json = new HttpOperationCatalog(List.of(pin)).json();
        assertFalse(json.contains("callbacks"));
        HttpOperationPin legacy = HttpOperationCatalog.read(
            json.replace("\"schemaVersion\":2", "\"schemaVersion\":1")).operations().getFirst();
        assertEquals(pin.operationFingerprint(), legacy.operationFingerprint());
        assertTrue(legacy.callbacks().isEmpty());
    }

    @Test
    void injectsIntoACopyAndRejectsEveryAuthoredValueIncludingNull() {
        var context = new ConnectorCallbackContext("job.completed", URI.create("https://app.test/callback"));
        ObjectNode wire = (ObjectNode) HttpPinnedJson.parse("{\"value\":\"input\"}");
        var injected = callback(202).target().inject(wire, context);
        assertFalse(wire.has("callbackUrl"));
        assertEquals(context.callbackUri().toString(), injected.path("callbackUrl").asText());
        wire.putNull("callbackUrl");
        assertThrows(IllegalArgumentException.class, () -> callback(202).target().inject(wire, context));
        wire.put("callbackUrl", "https://untrusted.test");
        assertThrows(IllegalArgumentException.class, () -> callback(202).target().inject(wire, context));
    }

    @Test
    void rejectsDuplicateAndNonCommandCallbacks() {
        assertThrows(IllegalArgumentException.class, () -> operation(List.of(callback(202), callback(202))));
        var pin = operation(List.of(callback(202)));
        assertThrows(IllegalArgumentException.class, () -> new HttpOperationPin(pin.operation(),
            ConnectorOperationKind.QUERY, pin.majorVersion(), pin.inputType(), pin.outputType(), pin.method(),
            pin.relativePathTemplate(), pin.parameters(), pin.requestBody(), pin.responses(), pin.security(),
            pin.requestSchema(), pin.requestMappingKey(), pin.providerIdempotencyKey(), pin.callbacks(), SOURCE));
    }

    @Test
    void preservesPropertyCountBoundsForAuthorableFieldsAndUnaffectedAncestors() {
        var schema = new HttpWireSchema("""
            {"type":"object","minProperties":1,"maxProperties":1,"properties":{"body":{
              "type":"object","minProperties":2,"maxProperties":2,"additionalProperties":false,
              "properties":{"value":{"type":"string"},"callbackUrl":{"type":"string"}},
              "required":["value","callbackUrl"]}},"required":["body"]}
            """);
        var authorable = schema.withoutRuntimeSuppliedPaths(List.of("/body/callbackUrl"));
        assertEquals(1, authorable.node().path("minProperties").asInt());
        assertEquals(1, authorable.node().path("maxProperties").asInt());
        assertEquals(1, authorable.node().path("properties").path("body").path("minProperties").asInt());
        assertEquals(1, authorable.node().path("properties").path("body").path("maxProperties").asInt());
        HttpWireValueValidator.validate(HttpPinnedJson.parse("{\"body\":{\"value\":\"input\"}}"), authorable);
        assertThrows(IllegalArgumentException.class, () -> HttpWireValueValidator.validate(
            HttpPinnedJson.parse("{\"body\":{}}"), authorable));
        assertEquals(2, schema.node().path("properties").path("body").path("minProperties").asInt());
        var onlyRuntime = new HttpWireSchema("""
            {"type":"object","minProperties":0,"maxProperties":1,"properties":{
              "body":{"type":"object","additionalProperties":false,"properties":{"callbackUrl":{"type":"string"}}}}}
            """).withoutRuntimeSuppliedPaths(List.of("/body/callbackUrl"));
        assertEquals(0, onlyRuntime.node().path("minProperties").asInt());
        assertEquals(0, onlyRuntime.node().path("maxProperties").asInt());
        assertThrows(IllegalArgumentException.class, () -> new HttpWireSchema("""
            {"type":"object","maxProperties":0,"properties":{"callbackUrl":{"type":"string"}}}
            """).withoutRuntimeSuppliedPaths(List.of("/callbackUrl")));
    }

    private HttpCallbackPin callback(int acknowledgement) {
        return new HttpCallbackPin("job.completed", "job.start", 1,
            new HttpCallbackInjectionTarget(HttpCallbackInjectionTarget.Location.BODY,
                List.of("callbackUrl"), Optional.empty()), "POST", "application/json", PAYLOAD,
            "http.job.completed.request", "JobCallback", HttpSecurityConstraint.none(), acknowledgement, true, SOURCE);
    }

    private HttpOperationPin operation(List<HttpCallbackPin> callbacks) {
        return new HttpOperationPin("job.start", ConnectorOperationKind.COMMAND, 1,
            "StartJob", "JobAccepted", "POST", "/jobs", List.of(),
            Optional.of(new HttpRequestBodyPin("application/json", Optional.empty(), true, REQUEST)),
            List.of(new HttpResponsePin("202", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                Optional.of("http.job.accepted"), Optional.empty(),
                Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), Optional.of(PAYLOAD))),
            HttpSecurityConstraint.none(), REQUEST, "http.job.start.request", Optional.empty(), callbacks, SOURCE);
    }
}
