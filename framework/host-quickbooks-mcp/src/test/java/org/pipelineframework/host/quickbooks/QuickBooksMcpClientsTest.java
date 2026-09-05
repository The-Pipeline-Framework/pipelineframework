package org.pipelineframework.host.quickbooks;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.mcp.McpClientConnection;
import io.modelcontextprotocol.spec.McpSchema;

class QuickBooksMcpClientsTest {
    @TempDir Path temporary;

    @Test
    void resolvesTenantBoundClientsAndReusesOneInitializedProcess() throws Exception {
        var first = registration("tenant-a", "instance-a");
        var second = registration("tenant-b", "instance-b");
        try (ExecutorService executor = Executors.newFixedThreadPool(4);
             var clients = host(List.of(first, second), executor)) {
            var calls = java.util.stream.IntStream.range(0, 8)
                .mapToObj(ignored -> clients.resolve(request("tenant-a")).toCompletableFuture()).toList();
            var connection = calls.getFirst().join();
            for (var call : calls) assertSame(connection.client(), call.join().client());
            assertEquals("tenant-a", result(connection).get("instance"));
            assertEquals("tenant-b", result(clients.resolve(request("tenant-b")).toCompletableFuture().join()).get("instance"));
            var error = assertThrows(CompletionException.class,
                () -> clients.resolve(request("unknown")).toCompletableFuture().join());
            assertEquals(ConnectionResolutionException.Kind.AUTHENTICATION_REQUIRED,
                ((ConnectionResolutionException) error.getCause()).kind());
            assertThrows(CompletionException.class, () -> clients.resolve(new ConnectionResolutionRequest<>(
                first.reference(), McpClientConnection.class, ConnectorExecutionContext.empty())).toCompletableFuture().join());
        }
    }

    @Test
    void nodeStateSurvivesRestartAndClosedClientCannotRespawn() throws Exception {
        var registration = registration("tenant", "100");
        McpClientConnection borrowed;
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            try (var clients = host(List.of(registration), executor)) {
                borrowed = clients.resolve(request("tenant")).toCompletableFuture().join();
                assertEquals(false, result(borrowed).get("restarted"));
            }
            assertThrows(RuntimeException.class, () -> borrowed.client().ping().block(Duration.ofSeconds(2)));
            try (var restarted = host(List.of(registration), executor)) {
                assertEquals(true, result(restarted.resolve(request("tenant")).toCompletableFuture().join()).get("restarted"));
            }
        }
        assertEquals(2, Files.readAllLines(registration.workingDirectory().resolve("starts")).size(),
            "A stale borrowed client must not spawn another process after ownership was released");
    }

    @Test
    void stalledInitializationDoesNotBlockAnotherTenantOrShutdown() throws Exception {
        var slow = registration("slow", "slow-instance");
        var fast = registration("fast", "fast-instance");
        var command = new java.util.ArrayList<>(command());
        command.add("wait-for-initialization");
        try (ExecutorService executor = Executors.newFixedThreadPool(4);
             var clients = host(List.of(withCommand(slow, command), fast), executor)) {
            var stalled = clients.resolve(request("slow")).toCompletableFuture();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (!Files.exists(slow.workingDirectory().resolve("starts")) && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertTrue(Files.exists(slow.workingDirectory().resolve("starts")));
            assertEquals("fast", result(clients.resolve(request("fast")).toCompletableFuture()
                .get(10, java.util.concurrent.TimeUnit.SECONDS)).get("instance"));
            assertFalse(stalled.isDone());
            java.util.concurrent.CompletableFuture.runAsync(clients::close, executor)
                .get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThrows(CompletionException.class, () -> clients.resolve(request("fast")).toCompletableFuture().join());
        }
    }

    @Test
    void rejectsDuplicateLocalInstanceBindings() throws Exception {
        var first = registration("tenant-a", "same-instance");
        var second = registration("tenant-b", "same-instance");
        assertThrows(IllegalArgumentException.class, () -> host(List.of(first, second), Runnable::run));
    }

    @Test
    void neverRespawnsAfterFailedInitialization() throws Exception {
        var registration = registration("tenant", "100");
        var command = new java.util.ArrayList<>(command());
        command.add("fail");
        try (var clients = new QuickBooksMcpClients(List.of(withCommand(registration, command)), Runnable::run, Duration.ofSeconds(2))) {
            assertThrows(CompletionException.class, () -> clients.resolve(request("tenant")).toCompletableFuture().join());
            assertThrows(CompletionException.class, () -> clients.resolve(request("tenant")).toCompletableFuture().join());
            assertEquals(1, Files.readAllLines(registration.workingDirectory().resolve("starts")).size());
        }
    }

    @Test
    void retriesShutdownUntilAStubbornChildActuallyExits() throws Exception {
        var registration = registration("tenant", "100");
        var command = new java.util.ArrayList<>(command());
        command.add("stubborn");
        // Allow cold JVM startup on CI; the shutdown hook still deterministically exceeds this bound.
        var first = new QuickBooksMcpClients(List.of(withCommand(registration, command)), Runnable::run, Duration.ofSeconds(15));
        try {
            first.resolve(request("tenant")).toCompletableFuture().join();
            assertThrows(ConnectionResolutionException.class, first::close);
            assertThrows(CompletionException.class, () -> first.resolve(request("tenant")).toCompletableFuture().join());
        } finally {
            Files.writeString(registration.workingDirectory().resolve("allow-exit"), "exit");
            first.close();
        }
        try (var replacement = host(List.of(registration), Runnable::run)) {
            assertEquals("tenant", result(replacement.resolve(request("tenant")).toCompletableFuture().join()).get("instance"));
        }
    }

    private Map<?, ?> result(McpClientConnection connection) {
        return (Map<?, ?>) connection.client().callTool(new McpSchema.CallToolRequest("search_customers", Map.of()))
            .block(Duration.ofSeconds(5)).structuredContent();
    }

    private QuickBooksMcpClients host(List<QuickBooksRegistration> registrations, java.util.concurrent.Executor executor) {
        return new QuickBooksMcpClients(registrations, executor, Duration.ofSeconds(15));
    }

    private List<String> command() {
        return List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp", System.getProperty("java.class.path"), QuickBooksFixture.class.getName());
    }

    private Path directory(String name) throws Exception {
        // macOS /var is a symlink; use the real path as the host provisioning contract requires.
        Path path = Files.createDirectory(temporary.resolve(name)).toRealPath();
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
        return path;
    }

    private QuickBooksRegistration registration(String tenant, String instance) throws Exception {
        return new QuickBooksRegistration(tenant, new ConnectionRef("quickbooks"), instance, command(), directory(tenant));
    }

    private QuickBooksRegistration withCommand(QuickBooksRegistration original, List<String> command) {
        return new QuickBooksRegistration(original.tenantId(), original.reference(), original.serverInstanceId(),
            command, original.workingDirectory());
    }

    private ConnectionResolutionRequest<McpClientConnection> request(String tenant) {
        return new ConnectionResolutionRequest<>(new ConnectionRef("quickbooks"), McpClientConnection.class,
            new ConnectorExecutionContext(Optional.of(tenant), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
    }
}
