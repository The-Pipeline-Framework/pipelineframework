package org.pipelineframework.connector.http;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
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
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.connector.QueryOutcome;

/** Executes release-pinned HTTP operations through ordinary Connector Query and Command contracts. */
@ApplicationScoped
public final class HttpConnector implements ConnectorProvider<HttpProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("http.client");
    private static final ConnectorConfigSchema<HttpProviderConfiguration> PROVIDER_SCHEMA =
        ConnectorConfigSchema.record(HttpProviderConfiguration.class, "http.client.provider", 1);

    private final AtomicReference<Optional<ActiveBinding>> active = new AtomicReference<>(Optional.empty());
    private final HttpRepresentationBindings representations;
    private final List<? extends ConnectorOperation> operations;

    public HttpConnector() {
        this(ConnectorProviderManifestLoader.metadataClassLoader(HttpConnector.class));
    }

    HttpConnector(ClassLoader classLoader) {
        this(HttpOperationCatalog.load(classLoader), HttpOperationBindingCatalog.load(classLoader),
            ConnectorProviderManifestLoader.load(classLoader), classLoader);
    }

    HttpConnector(
        HttpOperationCatalog pins,
        HttpOperationBindingCatalog bindings,
        ConnectorProviderManifestCatalog manifests,
        ClassLoader classLoader
    ) {
        validatePins(pins, bindings, manifests);
        representations = new HttpRepresentationBindings(bindings, classLoader);
        operations = pins.operations().stream().map(this::operation).toList();
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
    public Optional<ConnectorConfigSchema<HttpProviderConfiguration>> configurationSchema() {
        return Optional.of(PROVIDER_SCHEMA);
    }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, HttpProviderConfiguration configuration) {
        active.set(Optional.of(new ActiveBinding(context, configuration)));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        active.set(Optional.empty());
        return CompletableFuture.completedStage(null);
    }

    private ConnectorOperation operation(HttpOperationPin pin) {
        return pin.kind().equals(ConnectorOperationKind.QUERY)
            ? new PinnedQueryOperation(pin) : new PinnedCommandOperation(pin);
    }

    private CompletionStage<HttpClientConnection> connection(ConnectorExecutionContext executionContext) {
        try {
            ActiveBinding binding = active.get().orElseThrow(() ->
                new IllegalStateException("HTTP connector binding is not active"));
            ConnectionResolver resolver = binding.context().connectionResolver().orElseThrow(() ->
                new ConnectionResolutionException("No host ConnectionResolver is configured for pinned HTTP"));
            CompletionStage<HttpClientConnection> stage = resolver.resolve(new ConnectionResolutionRequest<>(
                binding.configuration().connection(), HttpClientConnection.class, executionContext));
            return Objects.requireNonNull(stage, "host ConnectionResolver returned a null HTTP stage");
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    private CompletionStage<HttpAuthorizationMaterial> authorization(
        HttpOperationPin pin,
        HttpClientConnection connection,
        ConnectorExecutionContext executionContext
    ) {
        Set<String> required = pin.security().requirements().stream()
            .map(HttpSecurityRequirement::scheme).collect(java.util.stream.Collectors.toUnmodifiableSet());
        if (!connection.securityCapabilities().containsAll(required)) {
            return CompletableFuture.failedStage(new HttpAuthorizationException(
                "host HTTP connection does not satisfy the selected security constraint"));
        }
        if (required.isEmpty()) return CompletableFuture.completedStage(HttpAuthorizationMaterial.none());
        try {
            CompletionStage<HttpAuthorizationMaterial> stage = connection.authorizationProvider().authorize(
                new HttpAuthorizationRequest(connection.baseUri(), pin.operation(), pin.security(), executionContext));
            return Objects.requireNonNull(stage, "HTTP authorization provider returned a null stage")
                .thenApply(value -> {
                    HttpAuthorizationMaterial material = Objects.requireNonNull(
                        value, "HTTP authorization provider returned null material");
                    try {
                        material.requireAllowedBy(pin.security());
                    } catch (IllegalArgumentException invalid) {
                        throw new HttpAuthorizationException(invalid.getMessage(), invalid);
                    }
                    return material;
                });
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    private CompletionStage<HttpResponse<byte[]>> send(
        HttpClientConnection connection,
        HttpRequest request,
        int maximumResponseBytes
    ) {
        try {
            return connection.client().sendAsync(request, new BoundedByteArrayBodyHandler(maximumResponseBytes));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(new HttpNotDispatchedException(failure));
        }
    }

    private HttpProviderConfiguration configuration() {
        return active.get().orElseThrow(() -> new IllegalStateException("HTTP connector binding is not active"))
            .configuration();
    }

    private final class PinnedQueryOperation implements QueryOperation<Object, ConnectorConfigurationDocument, Object> {
        private final HttpOperationPin pin;

        private PinnedQueryOperation(HttpOperationPin pin) {
            this.pin = pin;
        }

        @Override public String id() { return pin.operation(); }
        @Override public int majorVersion() { return pin.majorVersion(); }
        @Override public QueryCapabilities capabilities() { return QueryCapabilities.conservative(); }

        @Override
        public CompletionStage<QueryOutcome<Object>> query(
            QueryInvocation<Object, ConnectorConfigurationDocument, Object> invocation
        ) {
            final HttpProviderConfiguration runtimeConfiguration;
            try {
                runtimeConfiguration = configuration();
            } catch (RuntimeException inactive) {
                return CompletableFuture.completedStage(
                    new QueryOutcome.TemporarilyUnavailable<>("http-connection-unavailable"));
            }
            final com.fasterxml.jackson.databind.JsonNode wire;
            try {
                wire = representations.toWire(pin.requestMappingKey(), invocation.input());
            } catch (RuntimeException invalid) {
                return CompletableFuture.completedStage(new QueryOutcome.TerminalFailure<>("http-invalid-request"));
            }
            return connection(invocation.executionContext()).thenCompose(connection ->
                authorization(pin, connection, invocation.executionContext()).thenCompose(material -> {
                    HttpRequest request;
                    try {
                        request = HttpRequestProjector.project(pin, wire, connection, material,
                            runtimeConfiguration, Optional.empty());
                    } catch (RuntimeException invalid) {
                        return CompletableFuture.completedStage(
                            (QueryOutcome<Object>) new QueryOutcome.TerminalFailure<>("http-invalid-request"));
                    }
                    return send(connection, request, runtimeConfiguration.effectiveMaxResponseBytes()).handle((response, failure) -> {
                        if (failure != null) return queryTransportFailure(failure);
                        if (!HttpRequestProjector.sameOrigin(connection.baseUri(), response.uri())) {
                            return new QueryOutcome.TerminalFailure<Object>("http-cross-origin-response");
                        }
                        try {
                            return HttpResponseInterpreter.query(pin, response, invocation.outputType(), representations,
                                runtimeConfiguration.effectiveMaxResponseBytes());
                        } catch (RuntimeException invalid) {
                            return new QueryOutcome.TerminalFailure<Object>("http-invalid-response");
                        }
                    });
                })).exceptionally(HttpConnector::queryConnectionFailure);
        }
    }

    private final class PinnedCommandOperation implements CommandOperation<Object, ConnectorConfigurationDocument, Object> {
        private final HttpOperationPin pin;
        private final CommandCapabilities capabilities;

        private PinnedCommandOperation(HttpOperationPin pin) {
            this.pin = pin;
            this.capabilities = commandCapabilities(pin);
        }

        @Override public String id() { return pin.operation(); }
        @Override public int majorVersion() { return pin.majorVersion(); }
        @Override public CommandCapabilities capabilities() { return capabilities; }

        @Override
        public CompletionStage<CommandOutcome<Object>> dispatch(
            CommandInvocation<Object, ConnectorConfigurationDocument> invocation
        ) {
            final HttpProviderConfiguration runtimeConfiguration;
            try {
                runtimeConfiguration = configuration();
            } catch (RuntimeException inactive) {
                return completedRetryable("http-connection-unavailable");
            }
            final com.fasterxml.jackson.databind.JsonNode wire;
            try {
                wire = representations.toWire(pin.requestMappingKey(), invocation.input());
            } catch (RuntimeException invalid) {
                return completedTerminal("http-invalid-request");
            }
            return connection(invocation.executionContext()).handle((connection, resolutionFailure) -> {
                if (resolutionFailure != null) return completedRetryable("http-connection-unavailable");
                return authorization(pin, connection, invocation.executionContext()).handle((material, authFailure) -> {
                    if (authFailure != null) return completedTerminal("http-authorization-failed");
                    HttpRequest request;
                    try {
                        request = HttpRequestProjector.project(pin, wire, connection, material,
                            runtimeConfiguration, invocation.dispatchIdentity());
                    } catch (RuntimeException invalid) {
                        return completedTerminal("http-invalid-request");
                    }
                    CompletionStage<HttpResponse<byte[]>> sent;
                    try {
                        sent = send(connection, request, runtimeConfiguration.effectiveMaxResponseBytes());
                    } catch (RuntimeException notDispatched) {
                        return completedRetryable("http-dispatch-not-started");
                    }
                    return sent.handle((response, dispatchFailure) -> {
                        if (dispatchFailure != null) {
                            return root(dispatchFailure) instanceof HttpNotDispatchedException
                                ? retryable("http-dispatch-not-started") : ambiguous("http-dispatch-ambiguous");
                        }
                        if (!HttpRequestProjector.sameOrigin(connection.baseUri(), response.uri())) {
                            return ambiguous("http-cross-origin-response");
                        }
                        try {
                            return HttpResponseInterpreter.command(pin, response, invocation.outputType(),
                                representations, runtimeConfiguration.effectiveMaxResponseBytes());
                        } catch (RuntimeException invalid) {
                            return ambiguous("http-response-ambiguous");
                        }
                    });
                }).thenCompose(stage -> stage);
            }).thenCompose(stage -> stage);
        }
    }

    private static QueryOutcome<Object> queryTransportFailure(Throwable failure) {
        return root(failure) instanceof HttpAuthorizationException
            ? new QueryOutcome.AuthenticationRequired<>("http-authorization-failed")
            : new QueryOutcome.TemporarilyUnavailable<>("http-temporarily-unavailable");
    }

    private static QueryOutcome<Object> queryConnectionFailure(Throwable failure) {
        return queryTransportFailure(failure);
    }

    private static CommandCapabilities commandCapabilities(HttpOperationPin pin) {
        CommandMachineConfirmation maximum = pin.responses().stream().map(HttpResponsePin::confirmation)
            .flatMap(Optional::stream).max(java.util.Comparator.comparingInt(Enum::ordinal))
            .orElse(CommandMachineConfirmation.NONE);
        return new CommandCapabilities(true, pin.providerIdempotencyKey().isPresent(), false,
            CommandExecutionPosture.UNSPECIFIED, maximum, false, Set.of());
    }

    private static CompletionStage<CommandOutcome<Object>> completedRetryable(String code) {
        return CompletableFuture.completedStage(retryable(code));
    }

    private static CompletionStage<CommandOutcome<Object>> completedTerminal(String code) {
        return CompletableFuture.completedStage(terminal(code));
    }

    private static CommandOutcome<Object> retryable(String code) {
        return new CommandOutcome.RetryableFailure<>(code, List.of());
    }

    private static CommandOutcome<Object> terminal(String code) {
        return new CommandOutcome.TerminalFailure<>(code, List.of());
    }

    private static CommandOutcome<Object> ambiguous(String code) {
        return new CommandOutcome.Ambiguous<>(code, CommandConfirmation.none(), Set.of(), List.of());
    }

    private static Throwable root(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) current = current.getCause();
        return current;
    }

    private static void validatePins(
        HttpOperationCatalog pins,
        HttpOperationBindingCatalog bindings,
        ConnectorProviderManifestCatalog manifests
    ) {
        Optional<ConnectorProviderArtifactDescriptor> imported = manifests.providers().stream()
            .filter(provider -> provider.provider().id().equals(PROVIDER_ID)).findFirst();
        if (pins.operations().isEmpty() && imported.isEmpty()) return;
        ConnectorProviderArtifactDescriptor provider = imported.orElseThrow(() ->
            new IllegalStateException("pinned HTTP operations have no matching Connector provider manifest"));
        if (provider.provider().version().major() != 1) {
            throw new IllegalStateException("pinned HTTP operations require http.client provider major version 1");
        }
        Map<String, ConnectorOperationDescriptor> descriptors = new HashMap<>();
        Set<String> referencedMappings = new java.util.HashSet<>();
        provider.operations().forEach(operation -> descriptors.put(
            operation.kind().value() + ":" + operation.id() + ":" + operation.majorVersion(), operation));
        for (HttpOperationPin pin : pins.operations()) {
            ConnectorOperationDescriptor descriptor = Optional.ofNullable(descriptors.remove(pin.identity())).orElseThrow(() ->
                new IllegalStateException("pinned HTTP operation is absent from Connector metadata: " + pin.identity()));
            var contract = descriptor.typeContract().orElseThrow(() ->
                new IllegalStateException("pinned HTTP operation has no canonical type contract: " + pin.identity()));
            if (!pin.inputType().equals(contract.inputType())
                || !Optional.of(pin.outputType()).equals(contract.outputType())) {
                throw new IllegalStateException("pinned HTTP operation type contract disagrees with Connector metadata: "
                    + pin.identity());
            }
            if (pin.kind().equals(ConnectorOperationKind.QUERY)) {
                if (descriptor.queryCardinality().orElseThrow() != QueryOperationCardinality.ONE_TO_ONE
                    || !descriptor.queryCapabilities().orElse(QueryCapabilities.conservative())
                        .equals(QueryCapabilities.conservative())) {
                    throw new IllegalStateException("pinned HTTP Query capabilities disagree with Connector metadata: "
                        + pin.identity());
                }
            } else if (!descriptor.commandCapabilities().orElseThrow().equals(commandCapabilities(pin))) {
                throw new IllegalStateException("pinned HTTP Command capabilities disagree with Connector metadata: "
                    + pin.identity());
            }
            referencedMappings.add(pin.requestMappingKey());
            pin.responses().stream().map(HttpResponsePin::mappingKey).flatMap(Optional::stream)
                .forEach(referencedMappings::add);
        }
        if (!descriptors.isEmpty()) {
            throw new IllegalStateException("Connector metadata contains an HTTP operation absent from private pins: "
                + descriptors.keySet().stream().sorted().findFirst().orElseThrow());
        }
        bindings.bindings().stream().map(HttpOperationRepresentationBinding::mappingKey)
            .filter(mappingKey -> !referencedMappings.contains(mappingKey)).findFirst().ifPresent(mappingKey -> {
                throw new IllegalStateException(
                    "compiler-resolved HTTP mapping is not referenced by an immutable operation pin: " + mappingKey);
            });
    }

    private record ActiveBinding(ConnectorRuntimeContext context, HttpProviderConfiguration configuration) {
        private ActiveBinding {
            context = Objects.requireNonNull(context, "HTTP runtime context must not be null");
            configuration = Objects.requireNonNull(configuration, "HTTP provider configuration must not be null");
        }
    }

    private static final class HttpAuthorizationException extends RuntimeException {
        private HttpAuthorizationException(String message) {
            super(message);
        }

        private HttpAuthorizationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final class HttpNotDispatchedException extends RuntimeException {
        private HttpNotDispatchedException(Throwable cause) {
            super(cause);
        }
    }
}
