package org.pipelineframework.connector.mcp;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/** Executes explicitly imported MCP tools through ordinary TPF connector semantics. */
@ApplicationScoped
public final class McpConnector implements ConnectorProvider<McpProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("mcp.client");
    private static final ConnectorConfigSchema<McpProviderConfiguration> PROVIDER_SCHEMA =
        ConnectorConfigSchema.record(McpProviderConfiguration.class, "mcp.client.provider", 1);
    private static final ObjectMapper JSON = PipelineJson.mapper();

    private final AtomicReference<Optional<ActiveBinding>> activeBinding =
        new AtomicReference<>(Optional.empty());
    private final Map<ClassLoader, CanonicalTypeCatalogue> catalogues = new ConcurrentHashMap<>();
    private final List<? extends ConnectorOperation> operations;

    public McpConnector() {
        McpImportedToolCatalog catalog = McpImportedToolCatalog.load(
            ConnectorProviderManifestLoader.metadataClassLoader(McpConnector.class));
        validatePins(catalog);
        operations = catalog.tools().stream().map(this::operation).toList();
    }

    private static void validatePins(McpImportedToolCatalog pins) {
        ClassLoader classLoader = ConnectorProviderManifestLoader.metadataClassLoader(McpConnector.class);
        ConnectorProviderManifestCatalog manifests = ConnectorProviderManifestLoader.load(classLoader);
        Optional<org.pipelineframework.connector.ConnectorProviderArtifactDescriptor> imported = manifests.providers()
            .stream().filter(provider -> PROVIDER_ID.equals(provider.provider().id())).findFirst();
        if (pins.tools().isEmpty() && imported.isEmpty()) {
            return;
        }
        var provider = imported.orElseThrow(() ->
            new IllegalStateException("pinned MCP tools have no matching Connector provider manifest"));
        Map<String, org.pipelineframework.connector.ConnectorOperationDescriptor> descriptors = provider.operations()
            .stream().collect(java.util.stream.Collectors.toMap(
                operation -> operation.kind().value() + ":" + operation.id() + ":" + operation.majorVersion(),
                java.util.function.Function.identity()));
        for (McpImportedTool pin : pins.tools()) {
            String identity = pin.kind().value() + ":" + pin.operation() + ":" + pin.majorVersion();
            var descriptor = Optional.ofNullable(descriptors.remove(identity)).orElseThrow(() ->
                new IllegalStateException("pinned MCP operation is absent from Connector metadata: " + identity));
            var contract = descriptor.typeContract().orElseThrow(() ->
                new IllegalStateException("imported MCP operation has no canonical type contract: " + identity));
            if (!pin.inputType().equals(contract.inputType())
                || !Optional.of(pin.outputType()).equals(contract.outputType())) {
                throw new IllegalStateException("pinned MCP operation type contract disagrees with Connector metadata: "
                    + identity);
            }
        }
        if (!descriptors.isEmpty()) {
            throw new IllegalStateException("Connector metadata contains an MCP operation absent from the private pin: "
                + descriptors.keySet().stream().sorted().findFirst().orElseThrow());
        }
    }

    @Override
    public ConnectorProviderId id() {
        return PROVIDER_ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return operations;
    }

    @Override
    public Optional<ConnectorConfigSchema<McpProviderConfiguration>> configurationSchema() {
        return Optional.of(PROVIDER_SCHEMA);
    }

    @Override
    public CompletionStage<Void> start(
        ConnectorRuntimeContext context,
        McpProviderConfiguration configuration
    ) {
        activeBinding.set(Optional.of(new ActiveBinding(context, configuration)));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        activeBinding.set(Optional.empty());
        return CompletableFuture.completedStage(null);
    }

    private ConnectorOperation operation(McpImportedTool tool) {
        return tool.kind().equals(ConnectorOperationKind.QUERY)
            ? new ImportedQueryOperation(tool)
            : new ImportedCommandOperation(tool);
    }

    private CompletionStage<McpClientConnection> resolve(ConnectorExecutionContext invocationContext) {
        ActiveBinding binding = activeBinding.get().orElseThrow(() ->
            new IllegalStateException("MCP connector binding is not active"));
        ConnectionResolver resolver = binding.runtimeContext().connectionResolver().orElseThrow(() ->
            new ConnectionResolutionException("No host ConnectionResolver is configured for MCP"));
        CompletionStage<McpClientConnection> stage = resolver.resolve(new ConnectionResolutionRequest<>(
            binding.configuration().connection(), McpClientConnection.class, invocationContext));
        return Objects.requireNonNull(stage, "host ConnectionResolver returned a null stage");
    }

    private static Map<String, Object> arguments(Object input) {
        return JSON.convertValue(input, new TypeReference<>() { });
    }

    private static CompletionStage<McpSchema.CallToolResult> call(
        McpClientConnection connection,
        McpImportedTool tool,
        Object input
    ) {
        try {
            return connection.client().callTool(new McpSchema.CallToolRequest(tool.mcpName(), arguments(input))).toFuture();
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    private Object validatedOutput(McpImportedTool tool, McpSchema.CallToolResult result, Class<?> outputType) {
        if (result != null && Boolean.TRUE.equals(result.isError())) {
            throw new McpToolReportedError();
        }
        if (result == null || result.structuredContent() == null) {
            throw new IllegalStateException("MCP tool returned no structured output");
        }
        try {
            ClassLoader loader = Optional.ofNullable(outputType.getClassLoader())
                .orElseGet(() -> ConnectorProviderManifestLoader.metadataClassLoader(McpConnector.class));
            CanonicalTypeCatalogue catalogue = catalogues.computeIfAbsent(loader, CanonicalTypeCatalogue::load);
            String canonical = catalogue.validateAndCanonicalize(
                localTypeName(tool.outputType()), JSON.writeValueAsString(result.structuredContent()));
            return JSON.readValue(canonical, outputType);
        } catch (McpToolReportedError failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("MCP tool returned invalid structured output", failure);
        }
    }

    private static String localTypeName(String importedType) {
        if (importedType.startsWith("<") && importedType.endsWith(">")) {
            String qualified = importedType.substring(1, importedType.length() - 1);
            return qualified.substring(qualified.lastIndexOf('.') + 1);
        }
        return importedType.substring(importedType.lastIndexOf('.') + 1);
    }

    private final class ImportedQueryOperation implements QueryOperation<Object, ConnectorConfigurationDocument, Object> {
        private final McpImportedTool tool;

        private ImportedQueryOperation(McpImportedTool tool) {
            this.tool = tool;
        }

        @Override
        public String id() {
            return tool.operation();
        }

        @Override
        public int majorVersion() {
            return tool.majorVersion();
        }

        @Override
        public QueryCapabilities capabilities() {
            // LIVE_ONLY still uses the normal Query capture/replay path; it only disables cache reuse.
            return QueryCapabilities.conservative();
        }

        @Override
        public CompletionStage<QueryOutcome<Object>> query(
            QueryInvocation<Object, ConnectorConfigurationDocument, Object> invocation
        ) {
            try {
                return resolve(invocation.executionContext())
                    .thenCompose(connection -> call(connection, tool, invocation.input()))
                    .<QueryOutcome<Object>>handle((result, failure) -> {
                        if (failure != null) {
                            return queryFailure(failure);
                        }
                        try {
                            return new QueryOutcome.Found<>(validatedOutput(tool, result, invocation.outputType()));
                        } catch (RuntimeException invalid) {
                            return new QueryOutcome.TerminalFailure<>("mcp-invalid-result");
                        }
                    });
            } catch (RuntimeException failure) {
                return CompletableFuture.completedStage(queryFailure(failure));
            }
        }
    }

    private final class ImportedCommandOperation implements CommandOperation<Object, ConnectorConfigurationDocument, Object> {
        private final McpImportedTool tool;

        private ImportedCommandOperation(McpImportedTool tool) {
            this.tool = tool;
        }

        @Override
        public String id() {
            return tool.operation();
        }

        @Override
        public int majorVersion() {
            return tool.majorVersion();
        }

        @Override
        public CommandCapabilities capabilities() {
            return new CommandCapabilities(
                true, false, false, CommandExecutionPosture.UNSPECIFIED,
                CommandMachineConfirmation.NONE, false, Set.of());
        }

        @Override
        public CompletionStage<CommandOutcome<Object>> dispatch(
            CommandInvocation<Object, ConnectorConfigurationDocument> invocation
        ) {
            try {
                return resolve(invocation.executionContext()).handle((connection, resolutionFailure) -> {
                    if (resolutionFailure != null) {
                        return CompletableFuture.completedStage(commandResolutionFailure(resolutionFailure));
                    }
                    return call(connection, tool, invocation.input())
                        .<CommandOutcome<Object>>handle((result, dispatchFailure) -> {
                            if (dispatchFailure != null) {
                                return new CommandOutcome.Ambiguous<>("mcp-dispatch-uncertain", List.of());
                            }
                            try {
                                Object output = validatedOutput(tool, result, invocation.outputType());
                                return new CommandOutcome.Succeeded<>(
                                    output, CommandConfirmation.none(), Set.of(), List.of());
                            } catch (RuntimeException invalid) {
                                return new CommandOutcome.Ambiguous<>("mcp-invalid-result", List.of());
                            }
                        });
                }).thenCompose(stage -> stage);
            } catch (RuntimeException failure) {
                return CompletableFuture.completedStage(commandResolutionFailure(failure));
            }
        }
    }

    private static QueryOutcome<Object> queryFailure(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (cause instanceof ConnectionResolutionException resolution) {
            return switch (resolution.kind()) {
                case AUTHENTICATION_REQUIRED -> new QueryOutcome.AuthenticationRequired<>("mcp-authentication-required");
                case TEMPORARILY_UNAVAILABLE -> new QueryOutcome.TemporarilyUnavailable<>("mcp-temporarily-unavailable");
                case CONFIGURATION -> new QueryOutcome.TerminalFailure<>("mcp-connection-misconfigured");
            };
        }
        return new QueryOutcome.TemporarilyUnavailable<>("mcp-temporarily-unavailable");
    }

    private static CommandOutcome<Object> commandResolutionFailure(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (cause instanceof ConnectionResolutionException resolution
            && resolution.kind() == ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE) {
            return new CommandOutcome.RetryableFailure<>("mcp-connection-unavailable", List.of());
        }
        return new CommandOutcome.TerminalFailure<>("mcp-connection-failed", List.of());
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record ActiveBinding(ConnectorRuntimeContext runtimeContext, McpProviderConfiguration configuration) {
        private ActiveBinding {
            runtimeContext = Objects.requireNonNull(runtimeContext, "MCP runtime context must not be null");
            configuration = Objects.requireNonNull(configuration, "MCP provider configuration must not be null");
        }
    }

    private static final class McpToolReportedError extends RuntimeException {
    }
}
