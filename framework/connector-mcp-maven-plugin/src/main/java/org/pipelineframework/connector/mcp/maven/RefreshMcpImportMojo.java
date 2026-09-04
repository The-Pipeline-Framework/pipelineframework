package org.pipelineframework.connector.mcp.maven;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.ConnectorConfigFieldDescriptor;
import org.pipelineframework.connector.ConnectorConfigSchemaDescriptor;
import org.pipelineframework.connector.ConnectorConfigValueType;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorOperationTypeContract;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderArtifacts;
import org.pipelineframework.connector.ConnectorProviderDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifest;
import org.pipelineframework.connector.ConnectorProviderManifestReader;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;

/** Explicitly refreshes selected MCP tools into deterministic release-pinned Connector metadata. */
@Mojo(name = "refresh-import", requiresProject = true, threadSafe = false)
public final class RefreshMcpImportMojo extends AbstractMojo {
    private static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("mcp.client");
    private static final String MANIFEST_PATH = "META-INF/pipeline/connector-providers.json";
    private static final String PIN_PATH = "META-INF/pipeline/mcp-tools.json";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Parameter(defaultValue = "${project.basedir}/src/main/resources", required = true)
    private File outputDirectory;

    @Parameter(required = true)
    private String transport;

    @Parameter
    private String command;

    @Parameter
    private List<String> arguments = List.of();

    /** Child-process environment key to host environment-variable name. Values are never pinned. */
    @Parameter
    private Map<String, String> environment = Map.of();

    @Parameter
    private String endpoint;

    /** HTTP header name to host environment-variable name. Values are never pinned. */
    @Parameter
    private Map<String, String> headers = Map.of();

    @Parameter(defaultValue = "PT30S")
    private String timeout;

    @Parameter(required = true)
    private List<McpToolMapping> tools = List.of();

    @Override
    public void execute() throws MojoExecutionException {
        Duration requestTimeout;
        try {
            requestTimeout = Duration.parse(timeout);
        } catch (RuntimeException failure) {
            throw new MojoExecutionException("MCP import timeout must be an ISO-8601 duration", failure);
        }
        if (tools == null || tools.isEmpty()) {
            throw new MojoExecutionException("MCP import must select at least one tool");
        }
        McpClientTransport clientTransport = createTransport();
        try (McpSyncClient client = McpClient.sync(clientTransport)
            .clientInfo(new McpSchema.Implementation("tpf-mcp-import", "1"))
            .requestTimeout(requestTimeout)
            .initializationTimeout(requestTimeout)
            .build()) {
            client.initialize();
            List<McpSchema.Tool> discovered = discover(client);
            ImportedArtifacts imported = importTools(discovered, tools);
            write(outputDirectory.toPath(), imported);
            Set<String> selected = tools.stream().map(mapping -> mapping.mcpName).collect(java.util.stream.Collectors.toSet());
            List<String> omitted = discovered.stream().map(McpSchema.Tool::name)
                .filter(name -> !selected.contains(name)).sorted().toList();
            getLog().info("Imported " + imported.operations().size() + " MCP tool(s); discovered-only tools omitted: "
                + omitted.size());
        } catch (MojoExecutionException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new MojoExecutionException("Unable to refresh pinned MCP import", failure);
        }
    }

    private McpClientTransport createTransport() throws MojoExecutionException {
        var mapper = new JacksonMcpJsonMapper(JSON);
        if ("stdio".equalsIgnoreCase(transport)) {
            if (command == null || command.isBlank()) {
                throw new MojoExecutionException("STDIO MCP import requires command");
            }
            ServerParameters parameters = ServerParameters.builder(command)
                .args(arguments == null ? List.of() : arguments)
                .env(resolveEnvironment(environment, "STDIO environment"))
                .build();
            // The refresh goal is the import host for this transient process and closes it with the client.
            return new StdioClientTransport(parameters, mapper);
        }
        if ("streamable-http".equalsIgnoreCase(transport)) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new MojoExecutionException("Streamable HTTP MCP import requires endpoint");
            }
            URI uri;
            try {
                uri = URI.create(endpoint);
            } catch (RuntimeException failure) {
                throw new MojoExecutionException("Invalid Streamable HTTP MCP endpoint", failure);
            }
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                || uri.getAuthority() == null || uri.getAuthority().isBlank()
                || uri.getUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new MojoExecutionException(
                    "Streamable HTTP MCP endpoint must be an absolute HTTP(S) URI without user info, query, or fragment");
            }
            String base = uri.getScheme() + "://" + uri.getAuthority();
            String resource = Optional.ofNullable(uri.getRawPath()).filter(value -> !value.isBlank()).orElse("/mcp");
            Map<String, String> resolvedHeaders = resolveEnvironment(headers, "HTTP headers");
            HttpRequest.Builder request = HttpRequest.newBuilder();
            resolvedHeaders.forEach(request::header);
            return HttpClientStreamableHttpTransport.builder(base)
                .endpoint(resource)
                .requestBuilder(request)
                .jsonMapper(mapper)
                .build();
        }
        throw new MojoExecutionException("MCP import transport must be stdio or streamable-http");
    }

    private static Map<String, String> resolveEnvironment(Map<String, String> names, String subject)
        throws MojoExecutionException {
        Map<String, String> result = new LinkedHashMap<>();
        if (names == null) {
            return result;
        }
        for (Map.Entry<String, String> entry : names.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            String value = System.getenv(entry.getValue());
            if (value == null) {
                throw new MojoExecutionException(subject + " references unset host environment variable " + entry.getValue());
            }
            result.put(entry.getKey(), value);
        }
        return result;
    }

    private static List<McpSchema.Tool> discover(McpSyncClient client) {
        List<McpSchema.Tool> result = new ArrayList<>();
        Set<String> seenCursors = new java.util.LinkedHashSet<>();
        String cursor = null;
        do {
            McpSchema.ListToolsResult page = cursor == null ? client.listTools() : client.listTools(cursor);
            result.addAll(page.tools());
            cursor = page.nextCursor();
            if (cursor != null && !cursor.isBlank() && !seenCursors.add(cursor)) {
                throw new IllegalStateException("MCP discovery repeated tool page cursor '" + cursor + "'");
            }
        } while (cursor != null && !cursor.isBlank());
        return result;
    }

    static ImportedArtifacts importTools(List<McpSchema.Tool> discovered, List<McpToolMapping> mappings) {
        Map<String, McpSchema.Tool> byName = new LinkedHashMap<>();
        for (McpSchema.Tool tool : discovered) {
            if (byName.putIfAbsent(tool.name(), tool) != null) {
                throw new IllegalArgumentException("MCP discovery returned duplicate tool name '" + tool.name() + "'");
            }
        }
        McpSchemaNormalizer normalizer = new McpSchemaNormalizer();
        List<ConnectorOperationDescriptor> operations = new ArrayList<>();
        Map<String, ProtocolTypeDescriptor> types = new LinkedHashMap<>();
        List<Map<String, Object>> pins = new ArrayList<>();
        Set<String> importedNames = new java.util.HashSet<>();
        for (McpToolMapping mapping : mappings.stream().sorted(Comparator.comparing(value -> value.operation)).toList()) {
            if (!importedNames.add(mapping.mcpName)) {
                throw new IllegalArgumentException("MCP tool is mapped more than once: " + mapping.mcpName);
            }
            McpSchema.Tool tool = Optional.ofNullable(byName.get(mapping.mcpName)).orElseThrow(() ->
                new IllegalArgumentException("selected MCP tool was not discovered: " + mapping.mcpName));
            if (tool.inputSchema() == null) {
                throw new IllegalArgumentException("selected MCP tool has no inputSchema: " + mapping.mcpName);
            }
            if (tool.outputSchema() == null) {
                throw new IllegalArgumentException("selected MCP tool has no outputSchema: " + mapping.mcpName);
            }
            ConnectorOperationKind kind = importKind(mapping.kind);
            List<ProtocolTypeDescriptor> input = normalizer.normalize(
                mapping.inputType, tool.inputSchema(), "MCP tool '" + mapping.mcpName + "' input");
            List<ProtocolTypeDescriptor> output = normalizer.normalize(
                mapping.outputType, tool.outputSchema(), "MCP tool '" + mapping.mcpName + "' output");
            java.util.stream.Stream.concat(input.stream(), output.stream()).forEach(type -> {
                ProtocolTypeDescriptor duplicate = types.putIfAbsent(type.identity().qualifiedName(), type);
                if (duplicate != null && !duplicate.equals(type)) {
                    throw new IllegalArgumentException("conflicting imported canonical type: " + type.identity());
                }
            });
            operations.add(descriptor(mapping, kind));
            Map<String, Object> pin = new LinkedHashMap<>();
            pin.put("mcpName", mapping.mcpName);
            pin.put("operation", mapping.operation);
            pin.put("kind", kind.value());
            pin.put("majorVersion", mapping.majorVersion);
            pin.put("input", mapping.inputType);
            pin.put("output", mapping.outputType);
            pins.add(pin);
        }
        return new ImportedArtifacts(
            operations.stream().sorted(Comparator.comparing(ConnectorOperationDescriptor::id)
                .thenComparing(value -> value.kind().value()).thenComparingInt(ConnectorOperationDescriptor::majorVersion)).toList(),
            types.values().stream().sorted(Comparator.comparing(value -> value.identity().qualifiedName())).toList(),
            List.copyOf(pins));
    }

    private static ConnectorOperationKind importKind(String value) {
        if ("query".equalsIgnoreCase(value) || ConnectorOperationKind.QUERY.value().equals(value)) {
            return ConnectorOperationKind.QUERY;
        }
        if ("command".equalsIgnoreCase(value) || ConnectorOperationKind.COMMAND.value().equals(value)) {
            return ConnectorOperationKind.COMMAND;
        }
        throw new IllegalArgumentException("MCP import kind must be query or command: " + value);
    }

    private static ConnectorOperationDescriptor descriptor(McpToolMapping mapping, ConnectorOperationKind kind) {
        Optional<CommandCapabilities> commandCapabilities = kind.equals(ConnectorOperationKind.COMMAND)
            ? Optional.of(new CommandCapabilities(true, false, false, CommandExecutionPosture.UNSPECIFIED,
                CommandMachineConfirmation.NONE, false, Set.of())) : Optional.empty();
        Optional<QueryCapabilities> queryCapabilities = kind.equals(ConnectorOperationKind.QUERY)
            ? Optional.of(QueryCapabilities.conservative()) : Optional.empty();
        return new ConnectorOperationDescriptor(
            mapping.operation, kind, mapping.majorVersion, Optional.empty(), commandCapabilities, queryCapabilities,
            kind.equals(ConnectorOperationKind.QUERY)
                ? Optional.of(QueryOperationCardinality.ONE_TO_ONE) : Optional.empty(),
            Optional.of(new ConnectorOperationTypeContract(mapping.inputType, Optional.of(mapping.outputType))));
    }

    static void write(Path root, ImportedArtifacts imported) throws MojoExecutionException {
        try {
            Path manifestPath = root.resolve(MANIFEST_PATH);
            List<ConnectorProviderArtifactDescriptor> providers = new ArrayList<>();
            if (Files.isRegularFile(manifestPath)) {
                try (var input = Files.newInputStream(manifestPath)) {
                    providers.addAll(ConnectorProviderManifestReader.read(input).providers().stream()
                        .filter(provider -> !PROVIDER_ID.equals(provider.provider().id())).toList());
                }
            }
            ConnectorConfigSchemaDescriptor providerSchema = new ConnectorConfigSchemaDescriptor(
                "mcp.client.provider", 1,
                List.of(new ConnectorConfigFieldDescriptor(
                    "connection", ConnectorConfigValueType.CONNECTION_REF, true)));
            providers.add(new ConnectorProviderArtifactDescriptor(
                new ConnectorProviderDescriptor(PROVIDER_ID, new ConnectorProviderVersion(1, 0), Optional.of(providerSchema)),
                imported.operations(), imported.types()));
            providers.sort(Comparator.comparing(provider -> provider.provider().id()));
            ConnectorProviderManifest manifest = new ConnectorProviderManifest(
                ConnectorProviderManifest.CURRENT_SCHEMA_VERSION, providers);
            atomicWrite(manifestPath, ConnectorProviderArtifacts.json(manifest));

            Map<String, Object> pin = new LinkedHashMap<>();
            pin.put("schemaVersion", 1);
            pin.put("provider", PROVIDER_ID.value());
            pin.put("tools", imported.pins());
            atomicWrite(root.resolve(PIN_PATH), JSON.writeValueAsString(pin) + "\n");
        } catch (IOException failure) {
            throw new MojoExecutionException("Unable to write pinned MCP import", failure);
        }
    }

    private static void atomicWrite(Path target, String value) throws IOException {
        Files.createDirectories(target.getParent());
        Path temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        Files.writeString(temporary, value, StandardCharsets.UTF_8);
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    record ImportedArtifacts(
        List<ConnectorOperationDescriptor> operations,
        List<ProtocolTypeDescriptor> types,
        List<Map<String, Object>> pins
    ) {
    }
}
