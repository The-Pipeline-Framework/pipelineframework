package org.pipelineframework.connector.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.ConnectorOperationKind;

class HttpOperationCatalogTest {
    private static final String SOURCE = "abcdef0123456789".repeat(4);
    private static final HttpWireSchema STRING = new HttpWireSchema("{\"type\":\"string\"}");
    private static final HttpWireSchema OBJECT = new HttpWireSchema(
        "{\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"type\":\"object\"}");

    @Test
    void roundTripsCanonicalQueryPin() {
        HttpOperationPin operation = query();

        HttpOperationCatalog restored = HttpOperationCatalog.read(new HttpOperationCatalog(List.of(operation)).json());

        assertEquals(List.of(operation), restored.operations());
        assertEquals(new HttpOperationCatalog(List.of(operation)).json(), restored.json());
        assertEquals(new HttpWireSchema("{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}"),
            new HttpWireSchema("{\"properties\":{\"a\":{\"type\":\"string\"}},\"type\":\"object\"}"));
    }

    @Test
    void derivesTheFingerprintFromTheSameNormalisedValuesStoredByThePin() {
        HttpOperationPin canonical = query();
        HttpOperationPin equivalent = new HttpOperationPin(
            "evidence.lookup", canonical.kind(), canonical.majorVersion(), " LookupArguments ",
            " LookupResult ", "post", " /evidence/{subject}/lookup ", canonical.parameters(),
            canonical.requestBody(), canonical.responses(), canonical.security(), canonical.requestSchema(),
            "http.lookup.request", canonical.providerIdempotencyKey(), SOURCE.toUpperCase(java.util.Locale.ROOT));

        assertEquals(canonical, equivalent);
    }

    @Test
    void rejectsMethodBasedCommandSemanticsAndParameterDrift() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> new HttpOperationPin(
            "evidence.lookup", ConnectorOperationKind.QUERY, 1, "LookupArguments", "LookupResult", "POST",
            "/evidence/{subject}", List.of(), Optional.empty(),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.RESULT,
                Optional.of("http.lookup.response"), Optional.empty(), Optional.empty(), Optional.of(OBJECT))),
            HttpSecurityConstraint.none(), OBJECT, "http.lookup.request", Optional.empty(), SOURCE));
        assertTrue(failure.getMessage().contains("slots must exactly match"));
    }

    @Test
    void acceptsExplicitCommandIdempotencyProjection() {
        HttpOperationPin operation = new HttpOperationPin(
            "evidence.record", ConnectorOperationKind.COMMAND, 1, "RecordArguments", "RecordResult", "POST",
            "/evidence", List.of(), Optional.of(new HttpRequestBodyPin("application/json", Optional.empty(), true, OBJECT)),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                Optional.of("http.record.response"), Optional.empty(),
                Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), Optional.of(OBJECT))),
            HttpSecurityConstraint.none(), OBJECT, "http.record.request",
            Optional.of(new HttpProviderIdempotencyKeyTarget(HttpParameterLocation.HEADER, "Idempotency-Key")), SOURCE);

        assertEquals(ConnectorOperationKind.COMMAND, operation.kind());
        assertTrue(operation.providerIdempotencyKey().isPresent());
    }

    @Test
    void rejectsNoneAsACommandSuccessConfirmation() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> new HttpOperationPin(
            "evidence.record", ConnectorOperationKind.COMMAND, 1, "RecordArguments", "RecordResult", "POST",
            "/evidence", List.of(), Optional.empty(),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                Optional.of("http.record.response"), Optional.empty(),
                Optional.of(CommandMachineConfirmation.NONE), Optional.of(OBJECT))),
            HttpSecurityConstraint.none(), OBJECT, "http.record.request", Optional.empty(), SOURCE));

        assertTrue(failure.getMessage().contains("non-NONE confirmation"));
    }

    @Test
    void rejectsReservedAndCollidingHeaders() {
        IllegalArgumentException reserved = assertThrows(IllegalArgumentException.class,
            () -> new HttpParameterPin("Authorization", HttpParameterLocation.HEADER, "token",
                HttpParameterStyle.SIMPLE, false, true, false, STRING));
        assertTrue(reserved.getMessage().contains("reserved header"));

        IllegalArgumentException collision = assertThrows(IllegalArgumentException.class, () -> new HttpOperationPin(
            "evidence.record", ConnectorOperationKind.COMMAND, 1, "RecordArguments", "RecordResult", "POST",
            "/evidence", List.of(new HttpParameterPin("Idempotency-Key", HttpParameterLocation.HEADER, "key",
                HttpParameterStyle.SIMPLE, false, true, false, STRING)), Optional.empty(),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                Optional.of("http.record.response"), Optional.empty(),
                Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), Optional.of(OBJECT))),
            HttpSecurityConstraint.none(), OBJECT, "http.record.request",
            Optional.of(new HttpProviderIdempotencyKeyTarget(HttpParameterLocation.HEADER, "Idempotency-Key")), SOURCE));
        assertTrue(collision.getMessage().contains("collides"));
    }

    @Test
    void rejectsUndefinedOpenApiStyleAndExplodeCombinations() {
        assertThrows(IllegalArgumentException.class, () -> new HttpParameterPin("filter",
            HttpParameterLocation.QUERY, "filter", HttpParameterStyle.DEEP_OBJECT, false, true, false, OBJECT));
        assertThrows(IllegalArgumentException.class, () -> new HttpParameterPin("values",
            HttpParameterLocation.QUERY, "values", HttpParameterStyle.PIPE_DELIMITED, true, true, false,
            new HttpWireSchema("{\"type\":\"array\"}")));
    }

    private static HttpOperationPin query() {
        return new HttpOperationPin(
            "evidence.lookup", ConnectorOperationKind.QUERY, 1, "LookupArguments", "LookupResult", "POST",
            "/evidence/{subject}/lookup", List.of(
                new HttpParameterPin("subject", HttpParameterLocation.PATH, "subject", HttpParameterStyle.SIMPLE,
                    false, true, false, STRING),
                new HttpParameterPin("limit", HttpParameterLocation.QUERY, "limit", HttpParameterStyle.FORM,
                    true, false, false, new HttpWireSchema("{\"type\":\"integer\"}"))),
            Optional.of(new HttpRequestBodyPin("application/json", Optional.of("criteria"), true, OBJECT)),
            List.of(
                new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.RESULT,
                    Optional.of("http.lookup.response"), Optional.empty(), Optional.empty(), Optional.of(OBJECT)),
                new HttpResponsePin("404", Optional.empty(), HttpResponseOutcome.EMPTY,
                    Optional.empty(), Optional.of("evidence-not-found"), Optional.empty(), Optional.empty())),
            new HttpSecurityConstraint(List.of(new HttpSecurityRequirement("oauth2", List.of("evidence.read"),
                List.of(new HttpAuthorizationTarget(HttpParameterLocation.HEADER, "Authorization"))))),
            OBJECT, "http.lookup.request", Optional.empty(), SOURCE);
    }
}
