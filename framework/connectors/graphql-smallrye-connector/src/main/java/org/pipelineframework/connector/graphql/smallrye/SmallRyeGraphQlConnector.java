package org.pipelineframework.connector.graphql.smallrye;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.graphql.client.GraphQLError;
import io.smallrye.graphql.client.Response;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.graphql.GraphQlDataJson;
import org.pipelineframework.connector.graphql.GraphQlError;
import org.pipelineframework.connector.graphql.GraphQlMutationOperation;
import org.pipelineframework.connector.graphql.GraphQlMutationRequest;
import org.pipelineframework.connector.graphql.GraphQlOperationConfiguration;
import org.pipelineframework.connector.graphql.GraphQlQueryOperation;
import org.pipelineframework.connector.graphql.GraphQlQueryRequest;
import org.pipelineframework.connector.graphql.GraphQlResponse;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;

/** SmallRye asynchronous dynamic-client adapter for application-pinned GraphQL operations. */
@ApplicationScoped
public final class SmallRyeGraphQlConnector implements ConnectorProvider<GraphQlProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("graphql.smallrye");
    private static final ConnectorConfigSchema<GraphQlProviderConfiguration> CONFIGURATION =
        ConnectorConfigSchema.record(GraphQlProviderConfiguration.class, "graphql.smallrye.provider", 1);
    private static final CommandConfirmation ACKNOWLEDGED =
        new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> VARIABLES = new TypeReference<>() { };

    private final ClassLoader resourceClassLoader;
    private final AtomicReference<ActiveBinding> active = new AtomicReference<>();
    private final GraphQlQueryOperation query = new Query();
    private final GraphQlMutationOperation mutation = new Mutation();

    public SmallRyeGraphQlConnector() {
        this(Thread.currentThread().getContextClassLoader());
    }

    SmallRyeGraphQlConnector(ClassLoader resourceClassLoader) {
        this.resourceClassLoader = Objects.requireNonNull(
            resourceClassLoader, "GraphQL resource classloader must not be null");
    }

    @Override public ConnectorProviderId id() { return PROVIDER_ID; }
    @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    @Override public Optional<ConnectorConfigSchema<GraphQlProviderConfiguration>> configurationSchema() {
        return Optional.of(CONFIGURATION);
    }
    @Override public Collection<? extends ConnectorOperation> operations() { return List.of(query, mutation); }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, GraphQlProviderConfiguration configuration) {
        try {
            ConnectionResolver resolver = context.connectionResolver().orElseThrow(() ->
                new ConnectionResolutionException("No host ConnectionResolver is configured for GraphQL"));
            Map<String, GraphQlOperationCatalog.LinkedOperation> operations =
                GraphQlOperationCatalog.load(configuration, resourceClassLoader);
            active.set(new ActiveBinding(resolver, configuration.connection(), operations));
            return CompletableFuture.completedStage(null);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        active.set(null);
        return CompletableFuture.completedStage(null);
    }

    private final class Query implements GraphQlQueryOperation {
        @Override
        public CompletionStage<QueryOutcome<GraphQlResponse>> query(
            QueryInvocation<GraphQlQueryRequest, ConnectorConfigurationDocument, GraphQlResponse> invocation
        ) {
            try {
                var operation = operation(invocation.input().operationKey(), GraphQlOperationKind.QUERY);
                Map<String, Object> variables = variables(invocation.input().variablesJson());
                return connection(invocation.executionContext()).thenCompose(connection ->
                    execute(connection.client(), operation, variables).thenApply(response ->
                        new QueryOutcome.Found<>(normalize(response), Optional.of(QueryObservation.live(
                            Optional.empty(), Optional.of(operation.operationName()), Optional.empty())))));
            } catch (RuntimeException failure) {
                return CompletableFuture.failedStage(failure);
            }
        }
    }

    private final class Mutation implements GraphQlMutationOperation {
        @Override
        public CommandCapabilities capabilities() {
            return new CommandCapabilities(false, false, false, CommandExecutionPosture.AUTOMATED,
                CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false, Set.of());
        }

        @Override
        public CompletionStage<CommandOutcome<GraphQlResponse>> dispatch(
            CommandInvocation<GraphQlMutationRequest, GraphQlOperationConfiguration> invocation
        ) {
            GraphQlOperationCatalog.LinkedOperation operation;
            Map<String, Object> variables;
            try {
                operation = operation(invocation.input().operationKey(), GraphQlOperationKind.MUTATION);
                variables = variables(invocation.input().variablesJson());
            } catch (IllegalStateException inactiveBinding) {
                return CompletableFuture.completedStage(retryable("graphql-binding-inactive"));
            } catch (IllegalArgumentException invalidOperation) {
                return CompletableFuture.completedStage(terminal("graphql-operation-not-allowed"));
            }
            return connection(invocation.executionContext())
                .handle((connection, failure) -> failure == null
                    ? CompletableFuture.completedStage(connection)
                    : CompletableFuture.<AuthenticatedGraphQlConnection>failedStage(failure))
                .thenCompose(stage -> stage)
                .handle((connection, failure) -> {
                    if (failure != null) {
                        return CompletableFuture.<CommandOutcome<GraphQlResponse>>completedStage(
                            retryable("graphql-connection-unavailable"));
                    }
                    try {
                        return execute(connection.client(), operation, variables)
                            .handle((response, dispatchFailure) -> {
                                if (dispatchFailure != null) return ambiguous("graphql-dispatch-ambiguous");
                                try {
                                    return (CommandOutcome<GraphQlResponse>) new CommandOutcome.Succeeded<>(
                                        normalize(response), ACKNOWLEDGED, List.of());
                                } catch (RuntimeException invalidResponse) {
                                    return ambiguous("graphql-response-ambiguous");
                                }
                            });
                    } catch (RuntimeException definitePreDispatchFailure) {
                        return CompletableFuture.<CommandOutcome<GraphQlResponse>>completedStage(
                            retryable("graphql-dispatch-not-started"));
                    }
                })
                .thenCompose(stage -> stage);
        }
    }

    private CompletionStage<AuthenticatedGraphQlConnection> connection(
        org.pipelineframework.connector.ConnectorExecutionContext context
    ) {
        ActiveBinding binding = active();
        CompletionStage<AuthenticatedGraphQlConnection> resolved = binding.resolver().resolve(
            new ConnectionResolutionRequest<>(binding.connection(), AuthenticatedGraphQlConnection.class, context));
        return Objects.requireNonNull(resolved, "host ConnectionResolver returned a null GraphQL stage");
    }

    private static CompletionStage<Response> execute(
        DynamicGraphQLClient client,
        GraphQlOperationCatalog.LinkedOperation operation,
        Map<String, Object> variables
    ) {
        Uni<Response> result = Objects.requireNonNull(
            client.executeAsync(operation.document(), variables, operation.operationName()),
            "SmallRye GraphQL client returned a null asynchronous result");
        return result.subscribeAsCompletionStage();
    }

    private static Map<String, Object> variables(GraphQlVariablesJson variables) {
        try {
            return JSON.readValue(variables.value(), VARIABLES);
        } catch (JsonProcessingException impossibleForValidatedWrapper) {
            throw new IllegalArgumentException("GraphQL variables wrapper contains invalid JSON",
                impossibleForValidatedWrapper);
        }
    }

    private GraphQlOperationCatalog.LinkedOperation operation(String key, GraphQlOperationKind expectedKind) {
        GraphQlOperationCatalog.LinkedOperation operation = active().operations().get(key);
        if (operation == null) throw new IllegalArgumentException("Unknown GraphQL operation key: " + key);
        if (operation.kind() != expectedKind) {
            throw new IllegalArgumentException(
                "GraphQL operation '" + key + "' is " + operation.kind() + " rather than " + expectedKind);
        }
        return operation;
    }

    private ActiveBinding active() {
        ActiveBinding binding = active.get();
        if (binding == null) throw new IllegalStateException("GraphQL binding is not active");
        return binding;
    }

    private static GraphQlResponse normalize(Response response) {
        Response actual = Objects.requireNonNull(response, "SmallRye GraphQL response must not be null");
        Optional<GraphQlDataJson> data = actual.hasData()
            ? Optional.of(new GraphQlDataJson(Objects.requireNonNull(
                actual.getData(), "SmallRye GraphQL data must not be null").toString()))
            : Optional.empty();
        List<GraphQlError> errors = actual.getErrors() == null ? List.of() : actual.getErrors().stream()
            .limit(GraphQlResponse.MAX_ERRORS + 1L)
            .map(SmallRyeGraphQlConnector::normalize)
            .toList();
        return new GraphQlResponse(data, errors);
    }

    private static GraphQlError normalize(GraphQLError error) {
        GraphQLError actual = Objects.requireNonNull(error, "SmallRye GraphQL error must not be null");
        return new GraphQlError(actual.getCode(), path(actual.getPath()), actual.getMessage());
    }

    private static List<String> path(Object[] path) {
        if (path == null) return List.of();
        return java.util.Arrays.stream(path)
            .limit(GraphQlError.MAX_PATH_SEGMENTS + 1L)
            .map(segment -> String.valueOf(Objects.requireNonNull(
                segment, "SmallRye GraphQL error path segment must not be null")))
            .toList();
    }

    private static CommandOutcome<GraphQlResponse> terminal(String code) {
        return new CommandOutcome.TerminalFailure<>(code, List.of());
    }

    private static CommandOutcome<GraphQlResponse> retryable(String code) {
        return new CommandOutcome.RetryableFailure<>(code, List.of());
    }

    private static CommandOutcome<GraphQlResponse> ambiguous(String code) {
        return new CommandOutcome.Ambiguous<>(code, List.of());
    }

    private record ActiveBinding(
        ConnectionResolver resolver,
        org.pipelineframework.connector.ConnectionRef connection,
        Map<String, GraphQlOperationCatalog.LinkedOperation> operations
    ) {
        ActiveBinding {
            resolver = Objects.requireNonNull(resolver, "GraphQL connection resolver must not be null");
            connection = Objects.requireNonNull(connection, "GraphQL connection reference must not be null");
            operations = Map.copyOf(Objects.requireNonNull(operations, "GraphQL operations must not be null"));
        }
    }
}
