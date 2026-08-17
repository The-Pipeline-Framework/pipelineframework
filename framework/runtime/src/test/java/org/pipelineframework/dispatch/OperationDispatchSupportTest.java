package org.pipelineframework.dispatch;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.type.CanonicalTypeCatalogue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OperationDispatchSupportTest {
    private final QueryStepSupport queries = mock(QueryStepSupport.class);
    private final CommandStepSupport commands = mock(CommandStepSupport.class);
    private final OperationDispatchSupport support = new OperationDispatchSupport(
        queries, commands, ignored -> { throw new AssertionError("Query dispatch must not resolve command generators"); },
        ignored -> catalogue());

    @Test
    void normalizesFoundIntoCanonicalResultObservation() {
        when(queries.queryOutcomeOneToOne(any(), any(), eq(ToolResult.class)))
            .thenReturn(Uni.createFrom().item(new QueryOutcome.Found<>(new ToolResult("r-1", 42))));

        OperationObservation.Result result = assertInstanceOf(OperationObservation.Result.class,
            support.dispatch(descriptor(), "payments", "charge.lookup",
                    "{\"note\":\"invoice\",\"amount\":42}", OperationObservation.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertEquals("found", result.value().outcome());
        assertEquals("found", result.value().code());
        assertEquals("ToolResult", result.value().resultType());
        assertEquals("{\"acceptedAmount\":42,\"receipt\":\"r-1\"}", result.value().resultJson());
    }

    @Test
    void preservesNotFoundAsAnEmptyObservation() {
        when(queries.queryOutcomeOneToOne(any(), any(), eq(ToolResult.class)))
            .thenReturn(Uni.createFrom().item(new QueryOutcome.NotFound<>("invoice-missing")));

        OperationObservation.Empty result = assertInstanceOf(OperationObservation.Empty.class,
            support.dispatch(descriptor(), "payments", "charge.lookup",
                    "{\"amount\":42,\"note\":\"invoice\"}", OperationObservation.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertEquals("not-found", result.value().outcome());
        assertEquals("invoice-missing", result.value().code());
    }

    @Test
    void routesCommandThroughExistingCommandSupportAndReturnsAResultObservation() {
        CommandIdGenerator<ToolArguments> generator = (descriptor, input) -> "charge-42";
        OperationDispatchSupport commandSupport = new OperationDispatchSupport(
            queries, commands, ignored -> generator, ignored -> catalogue());
        doReturn(Uni.createFrom().item(new ToolResult("r-2", 42)))
            .when(commands).execute(any(org.pipelineframework.command.CommandDescriptor.class), any(), any());

        OperationObservation.Result result = assertInstanceOf(OperationObservation.Result.class,
            commandSupport.dispatch(commandDescriptor(), "payments", "charge.create",
                    "{\"amount\":42,\"note\":\"invoice\"}", OperationObservation.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertEquals("succeeded", result.value().outcome());
        assertEquals("tpf:command", result.value().kind());
        assertEquals("{\"acceptedAmount\":42,\"receipt\":\"r-2\"}", result.value().resultJson());
        verify(queries, never()).queryOutcomeOneToOne(any(), any(), any());
    }

    @Test
    void rejectsInvalidArgumentsAndUnexposedTargetsBeforeProviderInvocation() {
        assertThrows(IllegalArgumentException.class, () -> support.dispatch(
                descriptor(), "payments", "charge.lookup", "{\"amount\":42}", OperationObservation.class)
            .await().atMost(Duration.ofSeconds(2)));
        assertThrows(IllegalArgumentException.class, () -> support.dispatch(
                descriptor(), "other", "charge.lookup", "{}", OperationObservation.class)
            .await().atMost(Duration.ofSeconds(2)));

        verify(queries, never()).queryOutcomeOneToOne(any(), any(), any());
    }

    private static OperationDispatchDescriptor descriptor() {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("acme.payments"), "charge.lookup", ConnectorOperationKind.QUERY, 1);
        DispatchCapability capability = new DispatchCapability(
            new BoundOperationReference(ConnectorBindingName.of("payments"), "charge.lookup"),
            identity,
            1,
            "ToolArguments",
            ToolArguments.class,
            "ToolResult",
            ToolResult.class,
            Map.of(),
            Optional.of(QueryCapabilities.conservative()),
            Optional.empty());
        return OperationDispatchDescriptor.of("Dispatch", List.of(capability));
    }

    private static OperationDispatchDescriptor commandDescriptor() {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("acme.payments"), "charge.create", ConnectorOperationKind.COMMAND, 1);
        DispatchCapability capability = new DispatchCapability(
            new BoundOperationReference(ConnectorBindingName.of("payments"), "charge.create"),
            identity,
            1,
            "ToolArguments",
            ToolArguments.class,
            "ToolResult",
            ToolResult.class,
            Map.of(),
            Optional.empty(),
            Optional.of(new DispatchCapability.CommandConfiguration(
                "example.ChargeCommandIdGenerator", CommandDuplicatePolicy.RETURN_RECORDED, CommandPolicy.none())));
        return OperationDispatchDescriptor.of("InvokeProposal", List.of(capability));
    }

    @SuppressWarnings("unchecked")
    private static CanonicalTypeCatalogue catalogue() {
        try {
            Map<String, Object> types = PipelineJson.mapper().readValue("""
                {
                  "ToolArguments": {"definition": {"id":"ToolArguments","kind":"record","fields":[
                    {"name":"amount","type":{"kind":"scalar","id":"int32"}},
                    {"name":"note","type":{"kind":"scalar","id":"string"}}]}},
                  "ToolResult": {"definition": {"id":"ToolResult","kind":"record","fields":[
                    {"name":"acceptedAmount","type":{"kind":"scalar","id":"int32"}},
                    {"name":"receipt","type":{"kind":"scalar","id":"string"}}]}}
                }
                """, Map.class);
            return CanonicalTypeCatalogue.fromCanonicalTypes(types);
        } catch (java.io.IOException failure) {
            throw new AssertionError(failure);
        }
    }
}
