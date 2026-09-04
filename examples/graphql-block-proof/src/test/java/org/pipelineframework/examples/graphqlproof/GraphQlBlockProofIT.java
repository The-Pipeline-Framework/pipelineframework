package org.pipelineframework.examples.graphqlproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.graphql.client.Response;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.pipelineframework.connector.graphql.GraphQlMutationRequest;
import org.pipelineframework.connector.graphql.GraphQlQueryRequest;
import org.pipelineframework.connector.graphql.GraphQlResult;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;
import org.pipelineframework.connector.graphql.smallrye.AuthenticatedGraphQlConnection;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
class GraphQlBlockProofIT {
    @InjectMock
    PrimaryGraphQlConnectionResolver connectionResolver;

    @Inject
    @Any
    Instance<StepOneToOne<GraphQlQueryRequest, GraphQlResult>> queryPipelines;

    @Inject
    @Any
    Instance<StepOneToOne<GraphQlMutationRequest, GraphQlResult>> mutationPipelines;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    private DynamicGraphQLClient client;
    private String executionId;

    @BeforeEach
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void configureApplicationConnection() {
        client = mock(DynamicGraphQLClient.class);
        var connection = new AuthenticatedGraphQlConnection(client);
        when(connectionResolver.resolve(any()))
            .thenReturn((CompletableFuture) CompletableFuture.completedFuture(connection));
        executionId = "graphql-proof-" + UUID.randomUUID();
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-a", executionId, 0));
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void importedQueryAndMutationUseNativeCaptureAndEffectPaths() {
        Response queryResponse = response("{\"customer\":{\"id\":\"7\",\"name\":\"Before\"}}");
        Response mutationResponse = response("{\"updateCustomer\":{\"id\":\"7\",\"name\":\"Ada\"}}");
        when(client.executeAsync(anyString(), anyMap(), eq("CustomerLookup")))
            .thenReturn(Uni.createFrom().item(queryResponse));
        when(client.executeAsync(anyString(), anyMap(), eq("CustomerUpdate")))
            .thenReturn(Uni.createFrom().item(mutationResponse));
        var query = new GraphQlQueryRequest("customer.lookup", new GraphQlVariablesJson("{\"id\":\"7\"}"));
        var mutation = new GraphQlMutationRequest("customer.update", "proof/customer-7/name-ada",
            new GraphQlVariablesJson("{\"id\":\"7\",\"name\":\"Ada\"}"));

        GraphQlResult firstQuery = invoke(() -> queryPipeline().applyOneToOne(query));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-a", executionId, 0));
        GraphQlResult replayedQuery = invoke(() -> queryPipeline().applyOneToOne(query));
        GraphQlResult firstMutation = invoke(() -> mutationPipeline().applyOneToOne(mutation));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-a", executionId, 0));
        GraphQlResult replayedMutation = invoke(() -> mutationPipeline().applyOneToOne(mutation));

        assertEquals(firstQuery, replayedQuery);
        assertEquals(firstMutation, replayedMutation);
        assertTrue(firstQuery.data().orElseThrow().value().contains("Before"));
        assertTrue(firstMutation.data().orElseThrow().value().contains("Ada"));
        verify(client, times(1)).executeAsync(anyString(), anyMap(), eq("CustomerLookup"));
        verify(client, times(1)).executeAsync(anyString(), anyMap(), eq("CustomerUpdate"));
    }

    @Test
    void generatedMetadataShowsStaticBlockProvenanceAndOrdinaryOperationsOnly() throws Exception {
        String contract = metadata("pipeline-contract.json");
        String bindings = metadata("connector-bindings.json");

        assertTrue(contract.contains("org.pipelineframework.graphql/graphql-query"), contract);
        assertTrue(contract.contains("org.pipelineframework.graphql/graphql-mutation"), contract);
        assertTrue(contract.contains("linkedDefinitionFingerprint"), contract);
        assertTrue(contract.contains("graphql.smallrye"), contract);
        assertTrue(contract.contains("execute.query"), contract);
        assertTrue(contract.contains("execute.mutation"), contract);
        assertTrue(bindings.contains("execute.query"), bindings);
        assertTrue(bindings.contains("execute.mutation"), bindings);
        String all = contract + bindings;
        assertFalse(all.contains("http://127.0.0.1"), all);
        assertFalse(all.contains("query CustomerLookup"), all);
        assertFalse(all.contains("mutation CustomerUpdate"), all);
        assertFalse(all.contains("GraphQlRuntime"), all);
        assertFalse(all.contains("BlockRuntime"), all);
    }

    @Test
    void consumerDependsOnTheBlockAndOwnsNoGraphQlTransportOrNormalizationImplementation() throws Exception {
        String pom = Files.readString(Path.of("pom.xml"));
        assertTrue(pom.contains("<artifactId>graphql</artifactId>"), pom);
        assertTrue(pom.contains("<artifactId>graphql-smallrye-connector</artifactId>"), pom);
        try (var files = Files.list(Path.of("src/main/java/org/pipelineframework/examples/graphqlproof"))) {
            assertEquals(List.of("PrepareProofMutationService.java", "PrimaryGraphQlConnectionResolver.java"),
                files.map(path -> path.getFileName().toString()).sorted().toList());
        }
    }

    private StepOneToOne<GraphQlQueryRequest, GraphQlResult> queryPipeline() {
        return queryPipelines.stream().findFirst()
            .orElseThrow(() -> new AssertionError("generated graphql-query invocation bean not found"));
    }

    private StepOneToOne<GraphQlMutationRequest, GraphQlResult> mutationPipeline() {
        return mutationPipelines.stream().findFirst()
            .orElseThrow(() -> new AssertionError("generated graphql-mutation invocation bean not found"));
    }

    private <T> T invoke(java.util.function.Supplier<Uni<T>> operation) {
        return invocationRuntime.invokeStepUni(null, null, operation).await().indefinitely();
    }

    private static String metadata(String name) throws Exception {
        return Files.readString(Path.of("target/classes/META-INF/pipeline", name));
    }

    private static Response response(String json) {
        Response response = mock(Response.class);
        JsonObject data = mock(JsonObject.class);
        when(data.toString()).thenReturn(json);
        when(response.hasData()).thenReturn(true);
        when(response.getData()).thenReturn(data);
        when(response.getErrors()).thenReturn(List.of());
        return response;
    }
}
