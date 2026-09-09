package org.pipelineframework.examples.graphqlproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.graphql.client.GraphQLError;
import io.smallrye.graphql.client.Response;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.pipelineframework.blocks.graphql.agent.GraphQlAgentCompletion;
import org.pipelineframework.blocks.graphql.agent.GraphQlAgentOperation;
import org.pipelineframework.blocks.graphql.agent.GraphQlAgentOperationGuide;
import org.pipelineframework.blocks.graphql.agent.GraphQlAgentState;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.connector.graphql.smallrye.AuthenticatedGraphQlConnection;
import org.pipelineframework.examples.graphqlproof.connector.GraphQlAgentProofRecorder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
class GraphQlBlockProofIT {
    private static final String EFFECT_SCOPE = "tenant-a/customer-7/update-profile";

    @InjectMock
    PrimaryGraphQlConnectionResolver connectionResolver;

    @Inject
    @Any
    Instance<StepOneToOne<GraphQlAgentState, GraphQlAgentCompletion>> pipelines;

    @Inject
    GraphQlAgentProofRecorder recorder;

    @Inject
    PipelineConfig config;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    private DynamicGraphQLClient client;
    private String executionId;

    @BeforeEach
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void configureApplicationConnections() {
        recorder.reset();
        config.maxRecursiveDepth(8);
        client = mock(DynamicGraphQLClient.class);
        var connection = new AuthenticatedGraphQlConnection(client);
        when(connectionResolver.resolve(any()))
            .thenReturn((CompletableFuture) CompletableFuture.completedFuture(connection));
        executionId = "graphql-proof-" + UUID.randomUUID();
        setExecutionContext();
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void packagedAgentQueriesMutatesCompletesAndReplaysWithoutRedispatch() {
        Response lookupResponse = response(
            "{\"customer\":{\"id\":\"customer-7\",\"name\":\"Before\"}}", List.of());
        Response updateResponse = response(
            "{\"updateCustomer\":{\"id\":\"customer-7\",\"name\":\"Ada Lovelace\"}}",
            List.of(error("PROFILE_WARNING", "Profile updated with a provider warning.")));
        when(client.executeAsync(anyString(), anyMap(), eq("CustomerLookup")))
            .thenReturn(Uni.createFrom().item(lookupResponse));
        when(client.executeAsync(anyString(), anyMap(), eq("CustomerUpdate")))
            .thenReturn(Uni.createFrom().item(updateResponse));
        GraphQlAgentState state = GraphQlAgentState.start(
            "Look up customer-7 and update the customer name to Ada Lovelace.", operationGuide(), EFFECT_SCOPE, 4);

        GraphQlAgentCompletion first = invoke(state).await().indefinitely();
        setExecutionContext();
        GraphQlAgentCompletion replay = invoke(state).await().indefinitely();

        assertEquals(first, replay);
        assertEquals("Customer lookup and update completed through persisted GraphQL operations.", first.summary());
        assertEquals(3, recorder.inferenceCount());
        assertEquals(List.of(0, 1, 2), recorder.turns());
        assertTrue(recorder.modelInputs().stream().allMatch(input -> !input.contains("effectScope")));
        assertTrue(recorder.modelInputs().stream().allMatch(input -> !input.contains("nextEffectKey")));
        assertTrue(recorder.modelInputs().stream().allMatch(input -> !input.contains(EFFECT_SCOPE)));
        String mutationToolSchemas = recorder.toolCatalogues().stream().flatMap(List::stream)
            .filter(tool -> tool.alias().equals("graphql_mutation"))
            .map(tool -> tool.inputSchemaJson()).distinct().reduce("", String::concat);
        assertFalse(mutationToolSchemas.contains("effectKey"), mutationToolSchemas);
        assertTrue(mutationToolSchemas.contains("operationKey"), mutationToolSchemas);
        assertTrue(mutationToolSchemas.contains("variablesJson"), mutationToolSchemas);
        assertFalse(mutationToolSchemas.contains("document"), mutationToolSchemas);
        assertFalse(mutationToolSchemas.contains("endpoint"), mutationToolSchemas);
        verify(client, times(1)).executeAsync(anyString(), anyMap(), eq("CustomerLookup"));
        verify(client, times(1)).executeAsync(anyString(), anyMap(), eq("CustomerUpdate"));
    }

    @Test
    void turnExhaustionStopsBeforeAnotherInferenceOrMutation() {
        Response lookupResponse = response(
            "{\"customer\":{\"id\":\"customer-7\",\"name\":\"Before\"}}", List.of());
        when(client.executeAsync(anyString(), anyMap(), eq("CustomerLookup")))
            .thenReturn(Uni.createFrom().item(lookupResponse));
        GraphQlAgentState state = GraphQlAgentState.start(
            "Inspect customer-7 once.", operationGuide(), EFFECT_SCOPE + "/bounded", 1);

        GraphQlAgentCompletion completion = invoke(state).await().indefinitely();

        assertEquals("GraphQL agent stopped after reaching its configured turn bound.", completion.summary());
        assertEquals(1, recorder.inferenceCount());
        assertEquals(List.of(0), recorder.turns());
        verify(client, times(1)).executeAsync(anyString(), anyMap(), eq("CustomerLookup"));
        verify(client, never()).executeAsync(anyString(), anyMap(), eq("CustomerUpdate"));
    }

    @Test
    void generatedMetadataShowsStaticAgentAuthorityAndNoRuntimeSecrets() throws Exception {
        String contract = metadata("pipeline-contract.json");
        String bindings = metadata("connector-bindings.json");
        String all = contract + bindings;

        assertTrue(contract.contains("org.pipelineframework.graphql/graphql-agent"), contract);
        assertTrue(contract.contains("linkedDefinitionFingerprint"), contract);
        assertTrue(contract.contains("resolvedCallables"), contract);
        assertTrue(contract.contains("llm.decide"), contract);
        assertTrue(contract.contains("graphql.read"), contract);
        assertTrue(contract.contains("graphql.write"), contract);
        assertTrue(bindings.contains("graphql_query"), bindings);
        assertTrue(bindings.contains("graphql_mutation"), bindings);
        assertTrue(bindings.contains("execute.query"), bindings);
        assertTrue(bindings.contains("execute.mutation"), bindings);
        assertTrue(bindings.contains("effectKey"), bindings);
        assertFalse(all.contains("http://127.0.0.1"), all);
        assertFalse(all.contains("query CustomerLookup"), all);
        assertFalse(all.contains("mutation CustomerUpdate"), all);
        assertFalse(all.contains(EFFECT_SCOPE), all);
        assertFalse(all.contains("deterministic-graphql-proof"), all);
        assertFalse(all.contains("GraphQlAgentRuntime"), all);
        assertFalse(all.contains("BlockRuntime"), all);
    }

    @Test
    void consumerOwnsOnlyBindingsConnectionAndPersistedDocuments() throws Exception {
        String pom = Files.readString(Path.of("pom.xml"));
        assertTrue(pom.contains("<artifactId>graphql-agent</artifactId>"), pom);
        assertTrue(pom.contains("<artifactId>graphql-block-proof-connectors</artifactId>"), pom);
        assertTrue(pom.contains("<artifactId>graphql-smallrye-connector</artifactId>"), pom);
        assertFalse(pom.contains("<artifactId>graphql</artifactId>"), pom);
        try (var files = Files.list(Path.of("src/main/java/org/pipelineframework/examples/graphqlproof"))) {
            assertEquals(List.of("PrimaryGraphQlConnectionResolver.java"),
                files.map(path -> path.getFileName().toString()).sorted().toList());
        }
    }

    private GraphQlAgentOperationGuide operationGuide() {
        return new GraphQlAgentOperationGuide(List.of(
            new GraphQlAgentOperation("customer.lookup", "QUERY", "Look up one customer by ID.",
                "Variables: id is a required customer ID."),
            new GraphQlAgentOperation("customer.update", "MUTATION", "Update one customer name.",
                "Variables: id and name are required.")));
    }

    private StepOneToOne<GraphQlAgentState, GraphQlAgentCompletion> pipeline() {
        return pipelines.stream()
            .filter(candidate -> Arrays.stream(candidate.getClass().getDeclaredFields())
                .noneMatch(field -> field.getType().equals(Provider.class)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("generated root graphql-agent bean not found among "
                + pipelines.stream().map(candidate -> candidate.getClass().getName()).toList()));
    }

    private Uni<GraphQlAgentCompletion> invoke(GraphQlAgentState state) {
        return invocationRuntime.invokeStepUni(null, null, () -> pipeline().applyOneToOne(state));
    }

    private void setExecutionContext() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-a", executionId, 0));
    }

    private static String metadata(String name) throws Exception {
        String resource = "META-INF/pipeline/" + name;
        try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource)) {
            if (stream == null) {
                throw new AssertionError("generated metadata resource not found: " + resource);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Response response(String json, List<GraphQLError> errors) {
        Response response = mock(Response.class);
        JsonObject data = mock(JsonObject.class);
        when(data.toString()).thenReturn(json);
        when(response.hasData()).thenReturn(true);
        when(response.getData()).thenReturn(data);
        when(response.getErrors()).thenReturn(errors);
        return response;
    }

    private static GraphQLError error(String code, String message) {
        GraphQLError error = mock(GraphQLError.class);
        when(error.getCode()).thenReturn(code);
        when(error.getPath()).thenReturn(new Object[] { "updateCustomer" });
        when(error.getMessage()).thenReturn(message);
        return error;
    }
}
