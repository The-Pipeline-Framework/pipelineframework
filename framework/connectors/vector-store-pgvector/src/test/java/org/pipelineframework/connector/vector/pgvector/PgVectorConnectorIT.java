package org.pipelineframework.connector.vector.pgvector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.vector.VectorSearchQueryOperation;
import org.pipelineframework.connector.vector.VectorSearchRequest;
import org.pipelineframework.connector.vector.VectorSearchResult;
import org.pipelineframework.connector.vector.VectorUpsertCommandOperation;
import org.pipelineframework.connector.vector.VectorUpsertRequest;
import org.testcontainers.containers.PostgreSQLContainer;

class PgVectorConnectorIT {
    private static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("pgvector/pgvector:pg17");
    private static PGSimpleDataSource dataSource;

    @BeforeAll static void startDatabase() throws Exception {
        POSTGRES.start();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION vector");
            statement.execute("CREATE TABLE rag_vectors (item_id text PRIMARY KEY, content text NOT NULL, embedding vector(3) NOT NULL, updated_command_id text NOT NULL)");
            statement.execute("CREATE TABLE rag_vector_commands (command_id text PRIMARY KEY, request_fingerprint text NOT NULL, item_id text NOT NULL)");
        }
        dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
    }

    @AfterAll static void stopDatabase() { POSTGRES.stop(); }

    @Test void persistsOrdersReplaysAndSurvivesConnectorRestart() {
        var connector = connector();
        upsert(connector, "c1", new VectorUpsertRequest("b", "second", List.of(1f, 0f, 0f)));
        upsert(connector, "c2", new VectorUpsertRequest("a", "first", List.of(1f, 0f, 0f)));
        upsert(connector, "c1", new VectorUpsertRequest("b", "second", List.of(1f, 0f, 0f)));
        assertThrows(Exception.class, () ->
            upsert(connector, "c1", new VectorUpsertRequest("b", "changed", List.of(1f, 0f, 0f))));

        var restarted = connector();
        var result = search(restarted, new VectorSearchRequest("q", "question", List.of(1f, 0f, 0f), 1));
        assertEquals(List.of("a"), result.matches().stream().map(match -> match.itemId()).toList());
        assertEquals("second", search(restarted,
            new VectorSearchRequest("q2", "question", List.of(1f, 0f, 0f), 5)).matches().get(1).content());
    }

    private static PgVectorConnector connector() {
        var connector = new PgVectorConnector(dataSource,
            PgVectorConnector.runtimeSettings("public", "rag_vectors", "rag_vector_commands"));
        connector.start(ConnectorRuntimeContext.empty(), new PgVectorProviderConfiguration(3)).toCompletableFuture().join();
        return connector;
    }

    @SuppressWarnings("unchecked")
    private static void upsert(PgVectorConnector connector, String commandId, VectorUpsertRequest request) {
        var operation = (VectorUpsertCommandOperation<PgVectorUpsertConfiguration>) connector.operations().stream()
            .filter(candidate -> candidate.id().equals("upsert")).findFirst().orElseThrow();
        var outcome = operation.dispatch(new CommandInvocation<>(request, new PgVectorUpsertConfiguration(),
            ConnectorExecutionContext.empty(), Optional.of(new CommandDispatchIdentity(commandId, commandId + "-attempt"))))
            .toCompletableFuture().join();
        assertInstanceOf(CommandOutcome.Succeeded.class, outcome);
    }

    private static VectorSearchResult search(PgVectorConnector connector, VectorSearchRequest request) {
        var operation = (VectorSearchQueryOperation) connector.operations().stream()
            .filter(candidate -> candidate.id().equals("search")).findFirst().orElseThrow();
        var outcome = operation.query(new QueryInvocation<>(request, ConnectorConfigurationDocument.empty(),
            VectorSearchResult.class, ConnectorExecutionContext.empty())).toCompletableFuture().join();
        return (VectorSearchResult) assertInstanceOf(QueryOutcome.Found.class, outcome).output();
    }
}
