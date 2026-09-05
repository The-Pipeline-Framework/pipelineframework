package org.pipelineframework.host.quickbooks;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpAsyncClient;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.mcp.McpClientConnection;

/**
 * Local-development STDIO host. Node owns all authorization and credential storage.
 * This adapter owns only tenant binding, initialized client reuse and process lifetime.
 * Close on a blocking host thread after draining invocations. Not a production connection manager.
 */
public final class QuickBooksMcpClients implements ConnectionResolver, AutoCloseable {
    private record Key(String tenant, ConnectionRef reference) { }
    private final Map<Key, QuickBooksRegistration> registrations;
    private final Map<Key, Session> sessions = new HashMap<>();
    private final Executor executor;
    private final Duration timeout;
    private boolean closed;

    public QuickBooksMcpClients(List<QuickBooksRegistration> registrations, Executor executor, Duration timeout) {
        this.executor = Objects.requireNonNull(executor);
        this.timeout = Objects.requireNonNull(timeout);
        if (timeout.compareTo(Duration.ofSeconds(1)) < 0 || timeout.compareTo(Duration.ofSeconds(60)) > 0
            || registrations.size() > 32) {
            throw new IllegalArgumentException("QuickBooks host limits exceeded");
        }
        Map<Key, QuickBooksRegistration> entries = new HashMap<>();
        var instances = new HashSet<String>();
        for (QuickBooksRegistration registration : registrations) {
            if (entries.putIfAbsent(new Key(registration.tenantId(), registration.reference()), registration) != null
                || !instances.add(registration.serverInstanceId())) {
                throw new IllegalArgumentException("QuickBooks connection ownership must be unique");
            }
        }
        this.registrations = Map.copyOf(entries);
    }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        if (!request.connectionType().equals(McpClientConnection.class)) {
            return CompletableFuture.failedFuture(failure(ConnectionResolutionException.Kind.CONFIGURATION));
        }
        Optional<String> tenant = request.invocationContext().tenantId();
        if (tenant.isEmpty()) {
            return CompletableFuture.failedFuture(failure(ConnectionResolutionException.Kind.AUTHENTICATION_REQUIRED));
        }
        try {
            return CompletableFuture.supplyAsync(() -> request.connectionType().cast(
                initialized(new Key(tenant.orElseThrow(), request.reference()))), executor);
        } catch (RuntimeException rejected) {
            return CompletableFuture.failedFuture(failure(ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE));
        }
    }

    private McpClientConnection initialized(Key key) {
        try {
            Session session;
            synchronized (this) {
                if (closed) { throw failure(ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE); }
                QuickBooksRegistration registration = Optional.ofNullable(registrations.get(key)).orElseThrow(
                    () -> failure(ConnectionResolutionException.Kind.AUTHENTICATION_REQUIRED));
                session = sessions.computeIfAbsent(key, ignored -> new Session(registration));
            }
            return session.connection();
        } catch (ConnectionResolutionException error) {
            throw error;
        } catch (Exception error) {
            // Do not attach process output, SDK errors, or paths.
            throw failure(ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE);
        }
    }

    @Override
    public void close() {
        Map<Key, Session> closing;
        synchronized (this) {
            closed = true;
            closing = Map.copyOf(sessions);
        }
        closing.values().forEach(Session::prepareClose);
        boolean failed = false;
        for (var entry : closing.entrySet()) {
            try {
                entry.getValue().close();
                synchronized (this) { sessions.remove(entry.getKey(), entry.getValue()); }
            } catch (Exception error) {
                // Retain uncertain sessions for a subsequent cleanup attempt.
                failed = true;
            }
        }
        if (failed) { throw failure(ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE); }
    }

    private final class Session {
        private final McpAsyncClient client;
        private final OwnedTransport transport;
        private final Mono<io.modelcontextprotocol.spec.McpSchema.InitializeResult> initialization;
        private final java.util.concurrent.atomic.AtomicBoolean stopping = new java.util.concurrent.atomic.AtomicBoolean();

        private Session(QuickBooksRegistration registration) {
            List<String> command = registration.command();
            ServerParameters parameters = ServerParameters.builder(command.getFirst())
                .args(command.subList(1, command.size())).build();
            // The Node deployment launcher supplies its own configuration. No host credentials/options.
            parameters.getEnv().clear();
            transport = new OwnedTransport(parameters, registration.workingDirectory());
            // Upstream stderr may include provider errors. It must not enter ordinary host logs.
            transport.setStdErrorHandler(ignored -> { });
            client = McpClient.async(transport).requestTimeout(timeout).build();
            initialization = client.initialize().cache();
        }

        private synchronized McpClientConnection connection() {
            if (stopping.get()) { throw failure(ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE); }
            initialization.block(timeout);
            client.ping().block(timeout);
            if (stopping.get()) { throw failure(ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE); }
            return new McpClientConnection(client);
        }

        private void prepareClose() {
            stopping.set(true);
            transport.prepareClose();
        }

        private void close() {
            // Completion waits for the child to exit. A stale SDK client may reinitialize, but this
            // one-shot transport can never launch again after the host closes it.
            synchronized (transport) {
                client.closeGracefully().block(timeout);
            }
        }
    }

    private final class OwnedTransport extends StdioClientTransport {
        private final Path directory;
        private final Object lifecycle = new Object();
        private boolean launched;
        private boolean stopping;

        private OwnedTransport(ServerParameters parameters, Path directory) {
            super(parameters, new JacksonMcpJsonMapper(new ObjectMapper()));
            this.directory = directory;
        }

        @Override
        public Mono<Void> connect(Function<Mono<JSONRPCMessage>, Mono<JSONRPCMessage>> handler) {
            return Mono.<Void>fromRunnable(() -> {
                synchronized (lifecycle) {
                    if (launched || stopping) {
                        throw failure(ConnectionResolutionException.Kind.TEMPORARILY_UNAVAILABLE);
                    }
                    launched = true;
                    // Serialize the actual process creation against shutdown, not just subscription.
                    super.connect(handler).block();
                }
            }).subscribeOn(Schedulers.boundedElastic());
        }

        private void prepareClose() {
            synchronized (lifecycle) {
                stopping = true;
            }
        }

        @Override
        protected ProcessBuilder getProcessBuilder() {
            ProcessBuilder builder = new ProcessBuilder();
            builder.environment().clear();
            builder.directory(directory.toFile());
            return builder;
        }
    }

    private ConnectionResolutionException failure(ConnectionResolutionException.Kind kind) {
        return new ConnectionResolutionException(kind, "QuickBooks host connection is unavailable");
    }
}
