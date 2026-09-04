package org.pipelineframework.blocks.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.connector.graphql.GraphQlDataJson;
import org.pipelineframework.connector.graphql.GraphQlError;
import org.pipelineframework.connector.graphql.GraphQlMutationRequest;
import org.pipelineframework.connector.graphql.GraphQlQueryRequest;
import org.pipelineframework.connector.graphql.GraphQlResponse;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;

class GraphQlBlockServicesTest {
    @Test void canonicalizesRequestsAndNormalizesResponses() {
        var query = CanonicalizeGraphQlQueryService.canonicalize(
            new GraphQlQueryRequest("customer.lookup", new GraphQlVariablesJson("{\"z\":1,\"a\":2}")));
        var mutation = CanonicalizeGraphQlMutationService.canonicalize(new GraphQlMutationRequest(
            "customer.update", "effect-7", new GraphQlVariablesJson("{\"name\":\"Ada\"}")));
        var result = org.pipelineframework.connector.graphql.GraphQlResult.from(new GraphQlResponse(
            Optional.of(new GraphQlDataJson("{\"customer\":{\"id\":\"7\"}}")),
            List.of(new GraphQlError("NOTICE", List.of("customer"), "normalized"))));

        assertEquals("{\"a\":2,\"z\":1}", query.variablesJson().value());
        assertEquals("effect-7", mutation.effectKey());
        assertEquals("notice", result.errors().getFirst().code());
    }

    @Test void derivesIdentityFromTheApplicationSuppliedEffectKey() {
        var request = new GraphQlMutationRequest(
            "customer.update", "tenant-a/customer-7/v2", GraphQlVariablesJson.empty());

        var descriptor = new CommandDescriptor("graphql-mutation", "execute.mutation", "input", "output",
            GraphQlEffectKeyCommandIdGenerator.class.getName(), CommandDuplicatePolicy.RETURN_RECORDED,
            java.util.Map.of());
        String commandId = new GraphQlEffectKeyCommandIdGenerator().commandId(descriptor, request);

        assertEquals("graphql:customer.update:tenant-a/customer-7/v2", commandId);
    }
}
