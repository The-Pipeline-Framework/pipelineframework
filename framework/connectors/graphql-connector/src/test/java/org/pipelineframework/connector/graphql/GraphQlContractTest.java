package org.pipelineframework.connector.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.QueryCacheability;
import org.pipelineframework.protocol.ProtocolTypeContributor;

class GraphQlContractTest {
    @Test void ownsNominalCanonicalJsonAndOperationContracts() {
        var json = new GraphQlVariablesJson(" { \"z\": 1, \"a\": {\"b\": true} } ");

        assertEquals("{\"a\":{\"b\":true},\"z\":1}", json.value());
        assertEquals("execute.query", new TestQueryOperation().id());
        assertEquals(QueryCacheability.LIVE_ONLY, new TestQueryOperation().capabilities().cacheability());
        assertEquals("execute.mutation", new TestMutationOperation().id());
        assertEquals(CommandExecutionPosture.UNSPECIFIED,
            new TestMutationOperation().capabilities().executionPosture());
        assertEquals(CommandMachineConfirmation.NONE,
            new TestMutationOperation().capabilities().maximumMachineConfirmation());
        assertThrows(IllegalArgumentException.class, () -> new GraphQlVariablesJson("[]"));
        assertThrows(IllegalArgumentException.class, () -> new GraphQlVariablesJson("null"));
        assertThrows(IllegalArgumentException.class, () -> new GraphQlDataJson("null"));
        assertThrows(IllegalArgumentException.class, () -> new GraphQlQueryRequest(" ", json));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphQlMutationRequest("customer.update", " ", json));
        assertEquals(List.of("operationKey", "variablesJson"), Arrays.stream(GraphQlQueryRequest.class
            .getRecordComponents()).map(java.lang.reflect.RecordComponent::getName).toList());
        assertEquals(List.of("operationKey", "effectKey", "variablesJson"), Arrays.stream(GraphQlMutationRequest.class
            .getRecordComponents()).map(java.lang.reflect.RecordComponent::getName).toList());
    }

    @Test void boundsAndSanitizesGraphQlErrors() {
        var error = new GraphQlError("BAD_USER_INPUT", List.of("customer", "0"), "bad\u0001 input");

        assertEquals("bad-user-input", error.code());
        assertEquals("bad input", error.message());
        assertThrows(IllegalArgumentException.class,
            () -> new GraphQlResponse(java.util.Optional.empty(), List.of()));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphQlResult(java.util.Optional.empty(), List.of()));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphQlError("code", java.util.Collections.nCopies(33, "field"), "bad"));
    }

    @Test void contributesPortableGraphQlVocabulary() {
        ProtocolTypeContributor contributor = ServiceLoader.load(ProtocolTypeContributor.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(GraphQlProtocolTypeContributor.class::isInstance)
            .findFirst().orElseThrow();
        var definitions = contributor.protocolTypes();

        assertEquals(7, definitions.size());
        assertTrue(definitions.stream().anyMatch(type -> type.identity().equals(GraphQlProtocolTypeContributor.RESULT)));
        var variables = definitions.stream()
            .filter(type -> type.identity().equals(GraphQlProtocolTypeContributor.VARIABLES_JSON))
            .findFirst().orElseThrow();
        assertTrue(variables.definition() instanceof PipelineTemplateTypeDefinition.WrapperType);
    }

    private static final class TestQueryOperation implements GraphQlQueryOperation {
        @Override
        public java.util.concurrent.CompletionStage<org.pipelineframework.connector.QueryOutcome<GraphQlResponse>> query(
            org.pipelineframework.connector.QueryInvocation<GraphQlQueryRequest,
                org.pipelineframework.connector.ConnectorConfigurationDocument, GraphQlResponse> invocation
        ) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestMutationOperation implements GraphQlMutationOperation {
        @Override
        public java.util.concurrent.CompletionStage<org.pipelineframework.connector.CommandOutcome<GraphQlResponse>> dispatch(
            org.pipelineframework.connector.CommandInvocation<GraphQlMutationRequest,
                GraphQlOperationConfiguration> invocation
        ) {
            throw new UnsupportedOperationException();
        }
    }
}
