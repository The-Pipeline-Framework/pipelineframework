package org.pipelineframework.blocks.graphql.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.pipelineframework.config.pipeline.PipelineJson;

class GraphQlAgentServicesTest {
    @Test
    void initialisesBoundedStateAndDerivesDeterministicEffectIdentity() {
        GraphQlAgentState state = state(3);

        GraphQlAgentTurnGate.Ready ready = assertInstanceOf(GraphQlAgentTurnGate.Ready.class,
            PrepareGraphQlAgentTurnService.prepare(state));

        assertEquals(0, ready.value().state().turn());
        assertEquals("117c0f72a0081cc909cbd0a246181d3a55a244eb687fe321d8ecb249398857aa",
            ready.value().nextEffectKey());
        assertThrows(IllegalArgumentException.class, () -> GraphQlAgentState.start(
            "update customer", guide(), "tenant-a/customer-7", 0));
        assertThrows(IllegalArgumentException.class, () -> GraphQlAgentState.start(
            "update customer", guide(), "tenant-a/customer-7", 17));
    }

    @Test
    void reducesCanonicalGraphQlResultAndCompletesAtTheTurnBound() throws Exception {
        GraphQlAgentState state = state(1);
        String context = PipelineJson.mapper().writeValueAsString(Map.of("state", state));
        var observation = new OperationObservation.Result(new OperationResultObservation(
            "primary-graphql", "execute.query", "tpf:query", 1, "found", "found",
            "{\"operationKey\":\"customer.lookup\",\"variablesJson\":\"{\\\"customerId\\\":\\\"customer-7\\\"}\"}",
            context, "<tpf.graphql.GraphQlResponse>",
            "{\"data\":\"{\\\"customer\\\":{\\\"id\\\":\\\"customer-7\\\"}}\",\"errors\":[]}"));

        GraphQlAgentProgress.Complete complete = assertInstanceOf(GraphQlAgentProgress.Complete.class,
            ReduceGraphQlAgentObservationService.reduce(observation));

        assertEquals("GraphQL agent stopped after reaching its configured turn bound.", complete.value().summary());
    }

    @Test
    void reducerPreservesTheNormalizedObservationForRecursion() throws Exception {
        GraphQlAgentState state = state(3);
        String context = PipelineJson.mapper().writeValueAsString(Map.of("state", state));
        var observation = new OperationObservation.Result(new OperationResultObservation(
            "primary-graphql", "execute.query", "tpf:query", 1, "found", "found",
            "{\"operationKey\":\"customer.lookup\",\"variablesJson\":\"{}\"}", context,
            "<tpf.graphql.GraphQlResponse>",
            "{\"data\":\"{\\\"customer\\\":{\\\"id\\\":\\\"customer-7\\\"}}\",\"errors\":[]}"));

        GraphQlAgentProgress.Recur recur = assertInstanceOf(GraphQlAgentProgress.Recur.class,
            ReduceGraphQlAgentObservationService.reduce(observation));

        assertEquals(1, recur.value().turn());
        assertEquals(1, recur.value().history().size());
        assertEquals("customer.lookup", recur.value().history().getFirst().operationKey());
        assertEquals("QUERY", recur.value().history().getFirst().kind());
        assertEquals("{\"customer\":{\"id\":\"customer-7\"}}", recur.value().history().getFirst().dataJson());
    }

    @Test
    void reducerAcceptsErrorOnlyGraphQlResponsesWithNullData() throws Exception {
        GraphQlAgentState state = state(3);
        String context = PipelineJson.mapper().writeValueAsString(Map.of("state", state));
        var observation = new OperationObservation.Result(new OperationResultObservation(
            "primary-graphql", "execute.query", "tpf:query", 1, "found", "found",
            "{\"operationKey\":\"customer.lookup\",\"variablesJson\":\"{}\"}", context,
            "<tpf.graphql.GraphQlResponse>",
            "{\"data\":null,\"errors\":[{\"code\":\"not-found\",\"path\":[],\"message\":\"Customer not found\"}]}"));

        GraphQlAgentProgress.Recur recur = assertInstanceOf(GraphQlAgentProgress.Recur.class,
            ReduceGraphQlAgentObservationService.reduce(observation));

        GraphQlAgentObservation reduced = recur.value().history().getFirst();
        assertEquals("{}", reduced.dataJson());
        assertEquals("not-found", reduced.errors().getFirst().code());
    }

    @Test
    void operationGuideRejectsDuplicateKeysAndStateRejectsUntrustedHistoryShapes() {
        GraphQlAgentOperation operation = guide().operations().getFirst();
        assertThrows(IllegalArgumentException.class,
            () -> new GraphQlAgentOperationGuide(List.of(operation, operation)));
        assertThrows(IllegalArgumentException.class, () -> new GraphQlAgentState(
            "update customer", guide(), "tenant-a/customer-7", 3, 1, List.of()));
    }

    private static GraphQlAgentState state(int maxTurns) {
        return GraphQlAgentState.start("update customer", guide(), "tenant-a/customer-7", maxTurns);
    }

    private static GraphQlAgentOperationGuide guide() {
        return new GraphQlAgentOperationGuide(List.of(
            new GraphQlAgentOperation("customer.lookup", "QUERY", "Look up a customer", "customerId is required"),
            new GraphQlAgentOperation("customer.update", "MUTATION", "Update a customer", "customerId and name are required")));
    }
}
