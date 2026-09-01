package org.pipelineframework.connector.vector.pgvector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import javax.sql.DataSource;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.pipelineframework.connector.BlockingOperation;
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

/** Production JDBC adapter for PostgreSQL with the pgvector extension. */
@ApplicationScoped
public final class PgVectorConnector implements ConnectorProvider<PgVectorProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("vector.store.pgvector");
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final ConnectorConfigSchema<PgVectorProviderConfiguration> PROVIDER_CONFIGURATION =
        ConnectorConfigSchema.record(PgVectorProviderConfiguration.class, "vector.store.pgvector.provider", 1);

    private final Supplier<DataSource> dataSourceSupplier;
    private final RuntimeSettings runtimeSettings;
    private final UpsertOperation upsert = new UpsertOperation();
    private final SearchOperation search = new SearchOperation();
    private volatile ActiveBinding active;

    public PgVectorConnector() {
        this.dataSourceSupplier = () -> {
            throw new IllegalStateException("No default Quarkus JDBC datasource is available for pgvector");
        };
        this.runtimeSettings = RuntimeSettings.defaults();
    }

    @Inject
    PgVectorConnector(Instance<AgroalDataSource> dataSources, PgVectorRuntimeConfiguration configuration) {
        Objects.requireNonNull(dataSources, "pgvector datasource handle must not be null");
        this.dataSourceSupplier = () -> {
            if (!dataSources.isResolvable()) {
                throw new IllegalStateException("No default Quarkus JDBC datasource is available for pgvector");
            }
            return dataSources.get();
        };
        this.runtimeSettings = new RuntimeSettings(
            configuration.schema(), configuration.table(), configuration.commandTable());
    }

    PgVectorConnector(DataSource dataSource, RuntimeSettings runtimeSettings) {
        this.dataSourceSupplier = () -> Objects.requireNonNull(dataSource, "pgvector datasource must not be null");
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
            DataSource dataSource = dataSource();
            ActiveBinding candidate = new ActiveBinding(
                dataSource,
                configuration.dimensions(),
                qualified(runtimeSettings.schema(), runtimeSettings.table()),
                qualified(runtimeSettings.schema(), runtimeSettings.commandTable()));
            return CompletableFuture.runAsync(() -> validateSchema(candidate), context.executor())
                .thenRun(() -> active = candidate);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        active = null;
        return CompletableFuture.completedStage(null);
    }

    private DataSource dataSource() {
        return Objects.requireNonNull(dataSourceSupplier.get(), "pgvector datasource supplier returned null");
    }

    private final class UpsertOperation
        implements VectorUpsertCommandOperation<PgVectorUpsertConfiguration>, BlockingOperation {
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
                String commandId = invocation.dispatchIdentity().orElseThrow(() ->
                    new IllegalArgumentException("pgvector upsert requires a command dispatch identity")).commandId();
                return CompletableFuture.completedStage(upsert(binding, commandId, invocation.input()));
            } catch (RuntimeException failure) {
                return CompletableFuture.failedStage(failure);
            }
        }
    }

    private final class SearchOperation implements VectorSearchQueryOperation, BlockingOperation {
        @Override
        public CompletionStage<QueryOutcome<VectorSearchResult>> query(
            QueryInvocation<VectorSearchRequest, org.pipelineframework.connector.ConnectorConfigurationDocument,
                VectorSearchResult> invocation
        ) {
            try {
                ActiveBinding binding = active();
                validateDimensions(invocation.input().values(), binding.dimensions());
                return CompletableFuture.completedStage(new QueryOutcome.Found<>(search(binding, invocation.input())));
            } catch (RuntimeException failure) {
                return CompletableFuture.failedStage(failure);
            }
        }
    }

    private CommandOutcome<VectorUpsertResult> upsert(
        ActiveBinding binding,
        String commandId,
        VectorUpsertRequest request
    ) {
        String fingerprint = PgVectorRequestFingerprint.of(request);
        try (Connection connection = binding.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                int inserted = insertCommand(connection, binding, commandId, fingerprint, request.itemId());
                if (inserted == 0) {
                    verifyRecordedCommand(connection, binding, commandId, fingerprint, request.itemId());
                } else {
                    writeVector(connection, binding, commandId, request);
                    verifyVector(connection, binding, request);
                }
                connection.commit();
                return success(request.itemId());
            } catch (SQLException failure) {
                rollback(connection, failure);
                throw new IllegalStateException("pgvector upsert transaction failed", failure);
            } catch (RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("pgvector upsert failed", failure);
        }
    }

    private int insertCommand(
        Connection connection,
        ActiveBinding binding,
        String commandId,
        String fingerprint,
        String itemId
    ) throws SQLException {
        String sql = "INSERT INTO " + binding.commandTable()
            + " (command_id, request_fingerprint, item_id) VALUES (?, ?, ?) ON CONFLICT (command_id) DO NOTHING";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, commandId);
            statement.setString(2, fingerprint);
            statement.setString(3, itemId);
            return statement.executeUpdate();
        }
    }

    private void verifyRecordedCommand(
        Connection connection,
        ActiveBinding binding,
        String commandId,
        String fingerprint,
        String itemId
    ) throws SQLException {
        String sql = "SELECT request_fingerprint, item_id FROM " + binding.commandTable()
            + " WHERE command_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, commandId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("pgvector command ledger lost command " + commandId);
                if (!fingerprint.equals(result.getString(1)) || !itemId.equals(result.getString(2))) {
                    throw new IllegalStateException("command ID was reused with different vector content: " + commandId);
                }
            }
        }
    }

    private void writeVector(
        Connection connection,
        ActiveBinding binding,
        String commandId,
        VectorUpsertRequest request
    ) throws SQLException {
        String sql = "INSERT INTO " + binding.vectorTable()
            + " (item_id, content, embedding, updated_command_id) VALUES (?, ?, CAST(? AS vector), ?) "
            + "ON CONFLICT (item_id) DO UPDATE SET content = EXCLUDED.content, embedding = EXCLUDED.embedding, "
            + "updated_command_id = EXCLUDED.updated_command_id";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, request.itemId());
            statement.setString(2, request.content());
            statement.setString(3, vectorLiteral(request.values()));
            statement.setString(4, commandId);
            statement.executeUpdate();
        }
    }

    private void verifyVector(Connection connection, ActiveBinding binding, VectorUpsertRequest request)
        throws SQLException {
        String sql = "SELECT content, embedding::text FROM " + binding.vectorTable() + " WHERE item_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, request.itemId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || !request.content().equals(result.getString(1))
                    || !request.values().equals(parseVector(result.getString(2)))) {
                    throw new IllegalStateException("pgvector read-after-write verification failed for " + request.itemId());
                }
            }
        }
    }

    private VectorSearchResult search(ActiveBinding binding, VectorSearchRequest request) {
        String sql = "WITH query_vector AS (SELECT CAST(? AS vector) AS value) "
            + "SELECT item_id, content, (1 - (embedding <=> query_vector.value))::real AS score "
            + "FROM " + binding.vectorTable() + ", query_vector "
            + "ORDER BY embedding <=> query_vector.value, item_id ASC LIMIT ?";
        List<VectorMatch> matches = new ArrayList<>();
        try (Connection connection = binding.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, vectorLiteral(request.values()));
            statement.setInt(2, request.limit());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    matches.add(new VectorMatch(result.getString(1), result.getString(2), result.getFloat(3)));
                }
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("pgvector search failed", failure);
        }
        return new VectorSearchResult(request.queryId(), request.queryText(), matches);
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

    private void validateSchema(ActiveBinding binding) {
        try (Connection connection = binding.dataSource().getConnection()) {
            requireSingleValue(connection,
                "SELECT extversion FROM pg_extension WHERE extname = 'vector'", "pgvector extension is not installed");
            requireSingleValue(connection,
                "SELECT to_regclass('" + binding.vectorTable().replace("\"", "") + "')",
                "pgvector table does not exist");
            requireSingleValue(connection,
                "SELECT to_regclass('" + binding.commandTable().replace("\"", "") + "')",
                "pgvector command table does not exist");
            String type = requireSingleValue(connection,
                "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
                    + "WHERE a.attrelid = '" + binding.vectorTable().replace("\"", "")
                    + "'::regclass AND a.attname = 'embedding' AND NOT a.attisdropped",
                "pgvector embedding column does not exist");
            if (!type.equals("vector(" + binding.dimensions() + ")")) {
                throw new IllegalStateException(
                    "pgvector embedding column must be vector(" + binding.dimensions() + ") but was " + type);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("pgvector schema validation failed", failure);
        }
    }

    private static String requireSingleValue(Connection connection, String sql, String missing) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet result = statement.executeQuery()) {
            if (!result.next() || result.getString(1) == null) throw new IllegalStateException(missing);
            return result.getString(1);
        }
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

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
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
        DataSource dataSource,
        int dimensions,
        String vectorTable,
        String commandTable
    ) {
    }

}
