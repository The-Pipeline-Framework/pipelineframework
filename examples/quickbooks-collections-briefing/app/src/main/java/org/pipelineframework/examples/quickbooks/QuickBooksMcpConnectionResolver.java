package org.pipelineframework.examples.quickbooks;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.host.quickbooks.QuickBooksMcpClients;
import org.pipelineframework.host.quickbooks.QuickBooksRegistration;

/** Deployment adapter for the explicitly configured local QuickBooks connection. */
@ApplicationScoped
public class QuickBooksMcpConnectionResolver implements ConnectionResolver {
    private record HostResources(ExecutorService executor, QuickBooksMcpClients clients) { }

    private final String reference;
    private final String tenantId;
    private final String serverInstanceId;
    private final Optional<String> nodeExecutable;
    private final Optional<String> serverEntryPoint;
    private final Optional<String> configuredWorkingDirectory;
    private Optional<HostResources> resources = Optional.empty();

    @Inject
    public QuickBooksMcpConnectionResolver(
        @ConfigProperty(name = "quickbooks.connection.reference") String reference,
        @ConfigProperty(name = "quickbooks.connection.tenant") String tenantId,
        @ConfigProperty(name = "quickbooks.connection.server-instance") String serverInstanceId,
        @ConfigProperty(name = "quickbooks.mcp.node") Optional<String> nodeExecutable,
        @ConfigProperty(name = "quickbooks.mcp.server") Optional<String> serverEntryPoint,
        @ConfigProperty(name = "quickbooks.mcp.working-directory") Optional<String> configuredWorkingDirectory
    ) {
        this.reference = reference;
        this.tenantId = tenantId;
        this.serverInstanceId = serverInstanceId;
        this.nodeExecutable = nodeExecutable.filter(value -> !value.isBlank());
        this.serverEntryPoint = serverEntryPoint.filter(value -> !value.isBlank());
        this.configuredWorkingDirectory = configuredWorkingDirectory.filter(value -> !value.isBlank());
    }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        try {
            return clients().resolve(request);
        } catch (RuntimeException invalidConfiguration) {
            return CompletableFuture.failedStage(new ConnectionResolutionException(
                ConnectionResolutionException.Kind.CONFIGURATION, "QuickBooks host connection is not configured"));
        }
    }

    public String tenantId() {
        return tenantId;
    }

    private synchronized QuickBooksMcpClients clients() {
        if (resources.isPresent()) {
            return resources.orElseThrow().clients();
        }
        Path node = absolutePath(nodeExecutable, "QUICKBOOKS_NODE");
        Path server = absolutePath(serverEntryPoint, "QUICKBOOKS_MCP_SERVER");
        Path workingDirectory = configuredWorkingDirectory
            .map(value -> absolutePath(Optional.of(value), "QUICKBOOKS_MCP_WORKING_DIRECTORY"))
            .orElseGet(() -> defaultWorkingDirectory(server));
        var registration = new QuickBooksRegistration(
            tenantId, new ConnectionRef(reference), serverInstanceId,
            List.of(node.toString(), server.toString()), workingDirectory);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            var clients = new QuickBooksMcpClients(List.of(registration), executor, Duration.ofSeconds(15));
            resources = Optional.of(new HostResources(executor, clients));
            return clients;
        } catch (RuntimeException failure) {
            executor.shutdownNow();
            throw failure;
        }
    }

    @PreDestroy
    synchronized void close() {
        resources.ifPresent(resource -> {
            try {
                resource.clients().close();
            } finally {
                resource.executor().shutdown();
            }
        });
        resources = Optional.empty();
    }

    private static Path absolutePath(Optional<String> value, String environmentVariable) {
        Path path = Path.of(value.orElseThrow(
            () -> new IllegalStateException("Set " + environmentVariable + " to an absolute path"))).normalize();
        if (!path.isAbsolute()) {
            throw new IllegalStateException(environmentVariable + " must be an absolute path");
        }
        return path;
    }

    private static Path defaultWorkingDirectory(Path server) {
        Path parent = Optional.ofNullable(server.getParent()).orElseThrow(
            () -> new IllegalStateException("QuickBooks MCP server entry point has no parent directory"));
        if (parent.getFileName().toString().equals("dist") && parent.getParent() != null) {
            return parent.getParent();
        }
        return parent;
    }
}
