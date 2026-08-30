package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ConnectorExecutionContextTest {
    private static final ConnectorOperationIdentity OPERATION = new ConnectorOperationIdentity(
        ConnectorProviderId.of("google.gmail"), "list.messages", ConnectorOperationKind.QUERY, 1);
    private static final ConnectorInvocationTarget TARGET = new ConnectorInvocationTarget(
        ConnectorBindingName.of("gmail-primary"), OPERATION);

    @Test
    void carriesManagedExecutionAndConnectorIdentityWithoutCredentialMaterial() {
        Instant deadline = Instant.parse("2026-08-30T12:00:00Z");

        ConnectorExecutionContext context = ConnectorExecutionContext.managed(
            "tenant-a",
            "execution-17",
            "mail-pipeline",
            "contract-3",
            "release-9",
            "ReadInbox",
            TARGET,
            Optional.of("correlation-5"),
            Optional.of("trace-7"),
            Optional.of(deadline));

        assertEquals(Optional.of("tenant-a"), context.tenantId());
        assertEquals(Optional.of("execution-17"), context.executionId());
        assertEquals(Optional.of("mail-pipeline"), context.pipelineId());
        assertEquals(Optional.of("contract-3"), context.contractVersion());
        assertEquals(Optional.of("release-9"), context.releaseVersion());
        assertEquals(Optional.of("ReadInbox"), context.stepId());
        assertEquals(Optional.of(TARGET), context.invocationTarget());
        assertEquals(Optional.of("correlation-5"), context.correlationId());
        assertEquals(Optional.of("trace-7"), context.traceId());
        assertEquals(Optional.of(deadline), context.deadline());
        assertEquals(ConnectorProviderId.of("google.gmail"),
            context.invocationTarget().orElseThrow().operation().providerId());
    }

    @Test
    void localTargetContextKeepsIdentityWhileLeavingManagedIdentityAbsent() {
        ConnectorExecutionContext context = ConnectorExecutionContext.forTarget("ReadInbox", TARGET);

        assertTrue(context.tenantId().isEmpty());
        assertTrue(context.executionId().isEmpty());
        assertEquals(Optional.of("ReadInbox"), context.stepId());
        assertEquals(Optional.of(TARGET), context.invocationTarget());
    }

    @Test
    void resolutionRequestKeepsReferenceTypeAndInvocationContextTogether() {
        ConnectorExecutionContext context = ConnectorExecutionContext.forTarget("ReadInbox", TARGET);

        ConnectionResolutionRequest<TestConnection> request = new ConnectionResolutionRequest<>(
            new ConnectionRef("gmail-primary"), TestConnection.class, context);

        assertEquals(new ConnectionRef("gmail-primary"), request.reference());
        assertEquals(TestConnection.class, request.connectionType());
        assertEquals(context, request.invocationContext());
    }

    @Test
    void rejectsWhitespaceBearingSemanticIdentity() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectorExecutionContext(
            Optional.of(" tenant-a "), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
    }

    private record TestConnection() implements ResolvedConnection {
    }
}
