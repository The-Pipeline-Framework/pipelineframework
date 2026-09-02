package org.pipelineframework.connector.vector.pgvector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.vector.VectorMatch;
import org.pipelineframework.connector.vector.VectorSearchQueryOperation;
import org.pipelineframework.connector.vector.VectorSearchRequest;
import org.pipelineframework.connector.vector.VectorSearchResult;
import org.pipelineframework.connector.vector.VectorUpsertCommandOperation;
import org.pipelineframework.connector.vector.VectorUpsertRequest;
import org.pipelineframework.connector.vector.VectorUpsertResult;

/** Production non-blocking PostgreSQL adapter for the pgvector extension. */
@ApplicationScoped
public final class PgVectorConnector implements ConnectorProvider<PgVectorProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("vector.store.pgvector");
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final ConnectorConfigSchema<PgVectorProviderConfiguration> PROVIDER_CONFIGURATION =
        ConnectorConfigSchema.record(PgVectorProviderConfiguration.class, "vector.store.pgvector.provider", 1);

    private final Supplier<PgPool> poolSupplier;
    private final RuntimeSettings runtimeSettings;
    private final UpsertOperation upsert = new UpsertOperation();
    private final SearchOperation search = new SearchOperation();
    private volatile ActiveBinding active;

    public PgVectorConnector() {
        this.poolSupplier = () -> {
            throw new IllegalStateException("No default Quarkus reactive PostgreSQL pool is available for pgvector");
        };
        this.runtimeSettings = RuntimeSettings.defaults();
    }

    @Inject
    PgVectorConnector(Instance<PgPool> pools, PgVectorRuntimeConfiguration configuration) {
        Objects.requireNonNull(pools, "pgvector reactive pool handle must not be null");
        this.poolSupplier = () -> {
            if (!pools.isResolvable()) {
                throw new IllegalStateException("No default Quarkus reactive PostgreSQL pool is available for pgvector");
            }
            return pools.get();
        };
        this.runtimeSettings = new RuntimeSettings(
            configuration.schema(), configuration.table(), configuration.commandTable());
    }

    PgVectorConnector(PgPool pool, RuntimeSettings runtimeSettings) {
        this.poolSupplier = () -> Objects.requireNonNull(pool, "pgvector reactive pool must not be null");
        this.runtimeSettings = runtimeSettings;
    }

    @Override public ConnectorProviderId id() { return PROVIDER_ID; }
    @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    @Override public Optional<ConnectorConfigSchema<PgVectorProviderConfiguration>> configurationSchema() {
        return Optional.of(PROVIDER_CONFIGURATION);
    }
    @Override public Collection<? extends ConnectorOperation> operations() { return List.of(upsert, search); }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, PgVectorProviderConfiguration configuration) {
        try {
            ActiveBinding candidate = new ActiveBinding(
                pool(),
                configuration.dimensions(),
                qualified(runtimeSettings.schema(), runtimeSettings.table()),
                qualified(runtimeSettings.schema(), runtimeSettings.commandTable()));
            return validateSchema(candidate)
                .invoke(() -> active = candidate)
                .subscribeAsCompletionStage();
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        active = null;
        return CompletableFuture.completedStage(null);
    }

    private PgPool pool() {
        return Objects.requireNonNull(poolSupplier.get(), "pgvector reactive pool supplier returned null");
    }

    private final class UpsertOperation implements VectorUpsertCommandOperation<PgVectorUpsertConfiguration> {
        private static final ConnectorConfigSchema<PgVectorUpsertConfiguration> CONFIGURATION =
            ConnectorConfigSchema.record(PgVectorUpsertConfiguration.class, "vector.store.pgvector.upsert", 1);

        @Override public Optional<ConnectorConfigSchema<PgVectorUpsertConfiguration>> configurationSchema() {
            return Optional.of(CONFIGURATION);
        }

        @Override public CommandCapabilities capabilities() {
            return new CommandCapabilities(true, true, false, CommandExecutionPosture.AUTOMATED,
                CommandMachineConfirmation.READ_AFTER_WRITE_VERIFIED, false, Set.of());
        }

        @Override
        public CompletionStage<CommandOutcome<VectorUpsertResult>> dispatch(
            CommandInvocation<VectorUpsertRequest, PgVectorUpsertConfiguration> invocation
        ) {
            try {
                ActiveBinding binding = active();
                validateDimensions(invocation.input().values(), binding.dimensions());
                String providerIdempotencyKey = invocation.dispatchIdentity().orElseThrow(() ->
                    new IllegalArgumentException("pgvector upsert requires a command dispatch identity"))
                    .providerIdempotencyKey();
                return upsert(binding, providerIdempotencyKey, invocation.input()).subscribeAsCompletionStage();
            } catch (RuntimeException failure) {
                return CompletableFuture.failedStage(failure);
            }
        }
    }

    private final class SearchOperation implements VectorSearchQueryOperation {
        @Override
        public CompletionStage<QueryOutcome<VectorSearchResult>> query(
            QueryInvocation<VectorSearchRequest, org.pipelineframework.connector.ConnectorConfigurationDocument,
                VectorSearchResult> invocation
        ) {
            try {
                ActiveBinding binding = active();
                validateDimensions(invocation.input().values(), binding.dimensions());
                return search(binding, invocation.input())
                    .map(result -> (QueryOutcome<VectorSearchResult>) new QueryOutcome.Found<>(result))
                    .subscribeAsCompletionStage();
            } catch (RuntimeException failure) {
                return CompletableFuture.failedStage(failure);
            }
        }
    }

    private Uni<CommandOutcome<VectorUpsertResult>> upsert(
        ActiveBinding binding,
        String commandId,
        VectorUpsertRequest request
    ) {
        String fingerprint = PgVectorRequestFingerprint.of(request);
        return binding.pool().withTransaction(connection ->
            insertCommand(connection, binding, commandId, fingerprint, request.itemId())
                .flatMap(inserted -> inserted == 0
                    ? verifyRecordedCommand(connection, binding, commandId, fingerprint, request.itemId())
                    : writeVector(connection, binding, commandId, request)
                        .flatMap(ignored -> verifyVector(connection, binding, request))))
            .replaceWith(success(request.itemId()));
    }

    private Uni<Integer> insertCommand(
        SqlConnection connection,
        ActiveBinding binding,
        String commandId,
        String fingerprint,
        String itemId
    ) {
        String sql = "INSERT INTO " + binding.commandTable()
            + " (command_id, request_fingerprint, item_id) VALUES ($1, $2, $3) "
            + "ON CONFLICT (command_id) DO NOTHING";
        return connection.preparedQuery(sql).execute(Tuple.of(commandId, fingerprint, itemId))
            .map(RowSet::rowCount);
    }

    private Uni<Void> verifyRecordedCommand(
        SqlConnection connection,
        ActiveBinding binding,
        String commandId,
        String fingerprint,
        String itemId
    ) {
        String sql = "SELECT request_fingerprint, item_id FROM " + binding.commandTable()
            + " WHERE command_id = $1";
        return connection.preparedQuery(sql).execute(Tuple.of(commandId)).invoke(rows -> {
            Row row = first(rows).orElseThrow(() ->
                new IllegalStateException("pgvector command ledger lost command " + commandId));
            if (!fingerprint.equals(row.getString(0)) || !itemId.equals(row.getString(1))) {
                throw new IllegalStateException("command ID was reused with different vector content: " + commandId);
            }
        }).replaceWithVoid();
    }

    private Uni<Void> writeVector(
        SqlConnection connection,
        ActiveBinding binding,
        String commandId,
        VectorUpsertRequest request
    ) {
        String sql = "INSERT INTO " + binding.vectorTable()
            + " (item_id, content, embedding, updated_command_id) VALUES ($1, $2, CAST($3 AS vector), $4) "
            + "ON CONFLICT (item_id) DO UPDATE SET content = EXCLUDED.content, embedding = EXCLUDED.embedding, "
            + "updated_command_id = EXCLUDED.updated_command_id";
        return connection.preparedQuery(sql).execute(
            Tuple.of(request.itemId(), request.content(), vectorLiteral(request.values()), commandId))
            .replaceWithVoid();
    }

    private Uni<Void> verifyVector(
        SqlConnection connection,
        ActiveBinding binding,
        VectorUpsertRequest request
    ) {
        String sql = "SELECT content, embedding::text FROM " + binding.vectorTable() + " WHERE item_id = $1";
        return connection.preparedQuery(sql).execute(Tuple.of(request.itemId())).invoke(rows -> {
            Optional<Row> result = first(rows);
            if (result.isEmpty() || !request.content().equals(result.orElseThrow().getString(0))
                || !request.values().equals(parseVector(result.orElseThrow().getString(1)))) {
                throw new IllegalStateException("pgvector read-after-write verification failed for " + request.itemId());
            }
        }).replaceWithVoid();
    }

    private Uni<VectorSearchResult> search(ActiveBinding binding, VectorSearchRequest request) {
        String sql = "WITH query_vector AS (SELECT CAST($1 AS vector) AS value) "
            + "SELECT item_id, content, (1 - (embedding <=> query_vector.value))::real AS score "
            + "FROM " + binding.vectorTable() + ", query_vector "
            + "ORDER BY embedding <=> query_vector.value, item_id ASC LIMIT $2";
        return binding.pool().preparedQuery(sql)
            .execute(Tuple.of(vectorLiteral(request.values()), request.limit()))
            .map(rows -> {
                List<VectorMatch> matches = new ArrayList<>();
                for (Row row : rows) {
                    matches.add(new VectorMatch(row.getString(0), row.getString(1), row.getFloat(2)));
                }
                return new VectorSearchResult(request.queryId(), request.queryText(), matches);
            });
    }

    private static CommandOutcome<VectorUpsertResult> success(String itemId) {
        return new CommandOutcome.Succeeded<>(new VectorUpsertResult(itemId),
            new CommandConfirmation(CommandMachineConfirmation.READ_AFTER_WRITE_VERIFIED, false), List.of());
    }

    private static void validateDimensions(List<Float> values, int dimensions) {
        if (values.size() != dimensions) {
            throw new IllegalArgumentException(
                "pgvector dimensions must be " + dimensions + " but were " + values.size());
        }
    }

    private Uni<Void> validateSchema(ActiveBinding binding) {
        String vectorTable = unquoted(binding.vectorTable());
        String commandTable = unquoted(binding.commandTable());
        return requireSingleValue(binding.pool(),
            "SELECT extversion FROM pg_extension WHERE extname = 'vector'", Tuple.tuple(),
            "pgvector extension is not installed")
            .flatMap(ignored -> requireSingleValue(binding.pool(),
                "SELECT to_regclass($1)::text", Tuple.of(vectorTable), "pgvector table does not exist"))
            .flatMap(ignored -> requireSingleValue(binding.pool(),
                "SELECT to_regclass($1)::text", Tuple.of(commandTable), "pgvector command table does not exist"))
            .flatMap(ignored -> requireSingleValue(binding.pool(),
                "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
                    + "WHERE a.attrelid = CAST($1 AS regclass) AND a.attname = 'embedding' AND NOT a.attisdropped",
                Tuple.of(vectorTable), "pgvector embedding column does not exist"))
            .invoke(type -> {
                if (!type.equals("vector(" + binding.dimensions() + ")")) {
                    throw new IllegalStateException(
                        "pgvector embedding column must be vector(" + binding.dimensions() + ") but was " + type);
                }
            })
            .replaceWithVoid();
    }

    private static Uni<String> requireSingleValue(SqlClient client, String sql, Tuple parameters, String missing) {
        return client.preparedQuery(sql).execute(parameters).map(rows -> first(rows)
            .map(row -> row.getString(0))
            .filter(Objects::nonNull)
            .orElseThrow(() -> new IllegalStateException(missing)));
    }

    private static Optional<Row> first(RowSet<Row> rows) {
        var iterator = rows.iterator();
        return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
    }

    private ActiveBinding active() {
        ActiveBinding binding = active;
        if (binding == null) throw new IllegalStateException("pgvector binding is not active");
        return binding;
    }

    static String vectorLiteral(List<Float> values) {
        StringBuilder literal = new StringBuilder("[");
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) literal.append(',');
            literal.append(Float.toString(values.get(index)));
        }
        return literal.append(']').toString();
    }

    static List<Float> parseVector(String literal) {
        String body = literal.substring(1, literal.length() - 1);
        if (body.isEmpty()) return List.of();
        String[] parts = body.split(",", -1);
        List<Float> values = new ArrayList<>(parts.length);
        for (String part : parts) values.add(Float.valueOf(part));
        return List.copyOf(values);
    }

    static RuntimeSettings runtimeSettings(String schema, String table, String commandTable) {
        return new RuntimeSettings(schema, table, commandTable);
    }

    private static String qualified(String schema, String table) {
        return quote(schema) + "." + quote(table);
    }

    private static String quote(String identifier) {
        String normalized = Objects.requireNonNull(identifier, "SQL identifier must not be null").trim();
        if (!IDENTIFIER.matcher(normalized).matches()) {
            throw new IllegalArgumentException("invalid PostgreSQL identifier: " + identifier);
        }
        return '"' + normalized + '"';
    }

    private static String unquoted(String qualified) {
        return qualified.replace("\"", "");
    }

    record RuntimeSettings(String schema, String table, String commandTable) {
        RuntimeSettings {
            quote(schema);
            quote(table);
            quote(commandTable);
        }

        static RuntimeSettings defaults() {
            return new RuntimeSettings("public", "rag_vectors", "rag_vector_commands");
        }
    }

    private record ActiveBinding(
        PgPool pool,
        int dimensions,
        String vectorTable,
        String commandTable
    ) {
    }
}
