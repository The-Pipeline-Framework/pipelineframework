package org.pipelineframework.query;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationBinder;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperationInvocationCoordinator;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.StreamingQueryOperation;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.telemetry.QueryObservationTelemetry;

/**
 * Runtime support for generated captured query client steps.
 */
@ApplicationScoped
public class QueryStepSupport {
    private final List<FrameworkQueryConnector> connectors;
    private final List<QueryCaptureStore> stores;
    private final Optional<ConnectorBindingRegistry> bindingRegistry;
    private final ConnectorRuntimeContext runtimeContext;
    private final QueryObservationTelemetry observationTelemetry;
    private final ObjectMapper json = PipelineJson.mapper();
    private final QueryCapturePayloadCodec capturePayloadCodec = new QueryCapturePayloadCodec(json);
    private final ConnectorOperationInvocationCoordinator invocationCoordinator =
        new ConnectorOperationInvocationCoordinator();

    @Inject
    public QueryStepSupport(
        Instance<FrameworkQueryConnector> connectors,
        Instance<QueryCaptureStore> stores,
        ConnectorBindingRegistry bindingRegistry,
        ConnectorRuntimeContext runtimeContext
    ) {
        this(
            toList(connectors), toList(stores), Optional.of(bindingRegistry), runtimeContext,
            QueryObservationTelemetry.global(), false);
    }

    public QueryStepSupport(Collection<FrameworkQueryConnector> connectors, Collection<QueryCaptureStore> stores) {
        this(connectors, stores, Optional.empty(), ConnectorRuntimeContext.empty(), QueryObservationTelemetry.global());
    }

    public QueryStepSupport(
        Collection<FrameworkQueryConnector> connectors,
        Collection<QueryCaptureStore> stores,
        ConnectorBindingRegistry bindingRegistry
    ) {
        this(
            connectors, stores, Optional.ofNullable(bindingRegistry), ConnectorRuntimeContext.empty(),
            QueryObservationTelemetry.global());
    }

    public QueryStepSupport(
        Collection<FrameworkQueryConnector> connectors,
        Collection<QueryCaptureStore> stores,
        ConnectorBindingRegistry bindingRegistry,
        ConnectorRuntimeContext runtimeContext
    ) {
        this(
            connectors, stores, Optional.ofNullable(bindingRegistry), runtimeContext,
            QueryObservationTelemetry.global());
    }

    QueryStepSupport(
        Collection<FrameworkQueryConnector> connectors,
        Collection<QueryCaptureStore> stores,
        Optional<ConnectorBindingRegistry> bindingRegistry,
        ConnectorRuntimeContext runtimeContext,
        QueryObservationTelemetry observationTelemetry
    ) {
        this(connectors, stores, bindingRegistry, runtimeContext, observationTelemetry, true);
    }

    private QueryStepSupport(
        Collection<FrameworkQueryConnector> connectors,
        Collection<QueryCaptureStore> stores,
        Optional<ConnectorBindingRegistry> bindingRegistry,
        ConnectorRuntimeContext runtimeContext,
        QueryObservationTelemetry observationTelemetry,
        boolean allowUnmanagedMemoryDefault
    ) {
        this.connectors = connectors == null ? List.of() : List.copyOf(connectors);
        this.stores = allowUnmanagedMemoryDefault && (stores == null || stores.isEmpty())
            ? List.of(new InMemoryQueryCaptureStore())
            : stores == null ? List.of() : List.copyOf(stores);
        if (!allowUnmanagedMemoryDefault && this.stores.size() != 1) {
            throw new QueryCaptureStoreException(
                "Exactly one QueryCaptureStore CDI bean is required, but found " + this.stores.size());
        }
        this.bindingRegistry = java.util.Objects.requireNonNull(
            bindingRegistry, "connector binding registry selection must not be null");
        this.runtimeContext = java.util.Objects.requireNonNull(runtimeContext, "connector runtime context must not be null");
        this.observationTelemetry = java.util.Objects.requireNonNull(
            observationTelemetry, "query observation telemetry must not be null");
    }

    public <I, O> Uni<O> queryOneToOne(Uni<QueryStepDescriptor> descriptor, I input, Class<O> outputType) {
        return descriptor.onItem().transformToUni(resolved -> queryOneToOne(resolved, input, outputType));
    }

    public <I, O> Multi<O> queryOneToMany(Uni<QueryStepDescriptor> descriptor, I input, Class<O> outputType) {
        return descriptor.onItem()
            .transformToMulti(resolved -> queryOneToMany(resolved, input, outputType));
    }

    public <I, O, E> Multi<O> queryOneToMany(
        Uni<QueryStepDescriptor> descriptor,
        I input,
        Class<O> outputType,
        Class<E> externalOutputType,
        Mapper<O, E> mapper
    ) {
        return descriptor.onItem().transformToMulti(resolved -> queryOneToMany(
            resolved, input, outputType, externalOutputType, mapper));
    }

    public <I, O> Multi<O> queryOneToMany(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType
    ) {
        return queryOneToManyInternal(descriptor, input, outputType, outputType, outputType::cast);
    }

    public <I, O, E> Multi<O> queryOneToMany(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType,
        Class<E> externalOutputType,
        Mapper<O, E> mapper
    ) {
        java.util.Objects.requireNonNull(mapper, "mapper must not be null");
        return queryOneToManyInternal(descriptor, input, outputType, externalOutputType, item -> {
            E external = externalOutputType.cast(item);
            O canonical = mapper.fromExternal(external);
            if (canonical == null) {
                throw new IllegalStateException(
                    "persistence representation mapper returned null for canonical output " + outputType.getName());
            }
            return outputType.cast(canonical);
        });
    }

    private <I, O, E> Multi<O> queryOneToManyInternal(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType,
        Class<E> providerOutputType,
        Function<Object, O> rowMapper
    ) {
        return Multi.createFrom().deferred(() -> {
            if (descriptor == null || descriptor.nativeSelector().isEmpty()) {
                return Multi.createFrom().failure(new IllegalArgumentException(
                    "streaming Query requires a native Query descriptor"));
            }
            if (!"ONE_TO_MANY".equalsIgnoreCase(descriptor.cardinality())) {
                return Multi.createFrom().failure(new IllegalArgumentException(
                    "streaming Query descriptor must declare ONE_TO_MANY cardinality"));
            }
            java.util.Objects.requireNonNull(outputType, "outputType must not be null");
            java.util.Objects.requireNonNull(providerOutputType, "providerOutputType must not be null");
            Optional<PipelineExecutionContext> context = PipelineExecutionContextHolder.get();
            if (context.isEmpty()) {
                return executeStreamingNative(descriptor, input, providerOutputType, rowMapper);
            }
            try {
                PipelineExecutionContext execution = context.orElseThrow();
                QueryCaptureStore store = resolveStore();
                String inputJson = json.writeValueAsString(normalizedKeyInput(input, descriptor.keyFields()));
                String captureKey = captureKey(execution, descriptor, inputJson);
                StreamingQueryCaptureRequest request = new StreamingQueryCaptureRequest(
                    execution.tenantId(),
                    execution.executionId(),
                    execution.currentStepIndex(),
                    descriptor.queryId(),
                    descriptor.version(),
                    captureKey,
                    inputJson,
                    outputType.getName());
                return openStreaming(store, request).onItem().transformToMulti(opened -> {
                    if (opened instanceof StreamingQueryCaptureOpen.Replay replay) {
                        return Multi.createFrom().publisher(replay.items())
                            .onItem().transform(item -> decodeStreamingCapture(item, outputType));
                    }
                    StreamingQueryCaptureWriter writer =
                        ((StreamingQueryCaptureOpen.Write) opened).writer();
                    return captureStreaming(
                        executeStreamingNative(descriptor, input, providerOutputType, rowMapper),
                        writer,
                        outputType);
                });
            } catch (Exception failure) {
                return Multi.createFrom().failure(failure);
            }
        });
    }

    /**
     * Executes a native Query whose provider returns an existing external representation of the
     * canonical step output. The mapper is applied before Query capture, so capture and replay
     * remain canonical even though the provider reads the external representation.
     */
    public <I, O, E> Uni<O> queryOneToOne(
        Uni<QueryStepDescriptor> descriptor,
        I input,
        Class<O> outputType,
        Class<E> externalOutputType,
        Mapper<O, E> mapper
    ) {
        return descriptor.onItem().transformToUni(resolved ->
            queryOneToOne(resolved, input, outputType, externalOutputType, mapper));
    }

    public <I, O, E> Uni<O> queryOneToOne(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType,
        Class<E> externalOutputType,
        Mapper<O, E> mapper
    ) {
        if (descriptor == null || descriptor.nativeSelector().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "mapped Query representations require a native Query descriptor"));
        }
        java.util.Objects.requireNonNull(outputType, "outputType must not be null");
        java.util.Objects.requireNonNull(externalOutputType, "externalOutputType must not be null");
        java.util.Objects.requireNonNull(mapper, "mapper must not be null");
        Optional<PipelineExecutionContext> context = PipelineExecutionContextHolder.get();
        if (context.isEmpty()) {
            return executeMappedNative(descriptor, input, outputType, externalOutputType, mapper, Optional.empty());
        }
        PipelineExecutionContext executionContext = context.orElseThrow();
        try {
            QueryCaptureStore store = resolveStore();
            String inputJson = json.writeValueAsString(normalizedKeyInput(input, descriptor.keyFields()));
            String captureKey = captureKey(executionContext, descriptor, inputJson);
            return getCaptured(store, captureKey).onItem().transformToUni(existing -> {
                if (existing.isPresent()) {
                    return replayCaptured(descriptor, existing.orElseThrow(), outputType);
                }
                NativeCapture capture = new NativeCapture(store, executionContext, captureKey, inputJson);
                return executeMappedNative(
                    descriptor, input, outputType, externalOutputType, mapper, Optional.of(capture));
            });
        } catch (Exception failure) {
            return Uni.createFrom().failure(failure);
        }
    }

    public <I, O> Uni<O> queryOneToOne(QueryStepDescriptor descriptor, I input, Class<O> outputType) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        Optional<PipelineExecutionContext> context = PipelineExecutionContextHolder.get();
        if (context.isEmpty()) {
            return executeLive(descriptor, input, outputType);
        }
        PipelineExecutionContext executionContext = context.orElseThrow();
        QueryCaptureStore store;
        String captureKey;
        String inputJson;
        try {
            store = resolveStore();
            inputJson = json.writeValueAsString(normalizedKeyInput(input, descriptor.keyFields()));
            captureKey = captureKey(executionContext, descriptor, inputJson);
        } catch (Exception ex) {
            return Uni.createFrom().failure(ex);
        }
        return getCaptured(store, captureKey)
            .onItem().transformToUni(existing -> {
                if (existing.isPresent()) {
                    return replayCaptured(descriptor, existing.get(), outputType);
                }
                return executeAndCapture(
                    descriptor, input, outputType, store, executionContext, captureKey, inputJson);
            });
    }

    /**
     * Executes a native Query while preserving its typed semantic outcome for a caller that can
     * interpret valid negative observations. Existing {@link #queryOneToOne} behavior is unchanged.
     */
    public <I, O> Uni<QueryOutcome<O>> queryOutcomeOneToOne(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType
    ) {
        if (descriptor == null || descriptor.nativeSelector().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "semantic Query outcomes require a native Query descriptor"));
        }
        Optional<PipelineExecutionContext> context = PipelineExecutionContextHolder.get();
        if (context.isEmpty()) {
            return executeNative(descriptor, input, outputType)
                .onItem().transformToUni(outcome -> preserveNativeOutcome(
                    descriptor, outputType, outcome, Optional.empty()));
        }
        PipelineExecutionContext executionContext = context.orElseThrow();
        try {
            QueryCaptureStore store = resolveStore();
            String inputJson = json.writeValueAsString(normalizedKeyInput(input, descriptor.keyFields()));
            String captureKey = captureKey(executionContext, descriptor, inputJson);
            return getCaptured(store, captureKey).onItem().transformToUni(existing -> {
                if (existing.isPresent()) {
                    return replayCapturedOutcome(descriptor, existing.orElseThrow(), outputType);
                }
                NativeCapture capture = new NativeCapture(store, executionContext, captureKey, inputJson);
                return executeNative(descriptor, input, outputType)
                    .onItem().transformToUni(outcome -> preserveNativeOutcome(
                        descriptor, outputType, outcome, Optional.of(capture)));
            });
        } catch (Exception failure) {
            return Uni.createFrom().failure(failure);
        }
    }

    private <I, O> Uni<O> executeLive(QueryStepDescriptor descriptor, I input, Class<O> outputType) {
        if (descriptor.nativeSelector().isPresent()) {
            return executeNative(descriptor, input, outputType)
                .onItem().transformToUni(outcome -> applyNativeOutcome(
                    descriptor, outputType, outcome, Optional.empty()));
        }
        try {
            FrameworkQueryConnector connector = resolveConnector(descriptor.connector());
            return executeConnector(connector, new QueryRequest<>(descriptor, input), outputType);
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
    }

    private <I, O, E> Uni<O> executeMappedNative(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType,
        Class<E> externalOutputType,
        Mapper<O, E> mapper,
        Optional<NativeCapture> capture
    ) {
        AtomicBoolean mappedWithinProvider = new AtomicBoolean();
        Function<E, O> localResultMapper = external -> {
            O canonical = mapper.fromExternal(external);
            if (canonical == null) {
                throw new IllegalStateException(
                    "persistence representation mapper returned null for canonical output " + outputType.getName());
            }
            O result = outputType.cast(canonical);
            mappedWithinProvider.set(true);
            return result;
        };
        return executeNative(descriptor, input, externalOutputType, Optional.of(localResultMapper))
            .onItem().transformToUni(outcome -> {
                QueryOutcome<Object> canonicalOutcome = mappedWithinProvider.get()
                    ? outcome
                    : mapFoundRepresentation(descriptor, outcome, outputType, externalOutputType, mapper);
                return applyNativeOutcome(descriptor, outputType, canonicalOutcome, capture);
            });
    }

    @SuppressWarnings("unchecked")
    private <O, E> QueryOutcome<Object> mapFoundRepresentation(
        QueryStepDescriptor descriptor,
        QueryOutcome<Object> outcome,
        Class<O> outputType,
        Class<E> externalOutputType,
        Mapper<O, E> mapper
    ) {
        if (!(outcome instanceof QueryOutcome.Found<Object> found)) {
            return outcome;
        }
        E external;
        try {
            external = externalOutputType.cast(found.output());
        } catch (ClassCastException failure) {
            String actualType = found.output() == null ? "null" : found.output().getClass().getName();
            throw new IllegalStateException(
                "native query operation " + descriptor.nativeSelector().orElseThrow().operationIdentity()
                    + " returned " + actualType
                    + " but persistence representation expected " + externalOutputType.getName(),
                failure);
        }
        O canonical = mapper.fromExternal(external);
        if (canonical == null) {
            throw new IllegalStateException(
                "persistence representation mapper returned null for canonical output " + outputType.getName());
        }
        return (QueryOutcome<Object>) (QueryOutcome<?>) new QueryOutcome.Found<>(
            outputType.cast(canonical), found.observation());
    }

    private <I, O> Uni<O> executeAndCapture(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType,
        QueryCaptureStore store,
        PipelineExecutionContext executionContext,
        String captureKey,
        String inputJson
    ) {
        if (descriptor.nativeSelector().isPresent()) {
            NativeCapture capture = new NativeCapture(store, executionContext, captureKey, inputJson);
            return executeNative(descriptor, input, outputType)
                .onItem().transformToUni(outcome -> applyNativeOutcome(
                    descriptor, outputType, outcome, Optional.of(capture)));
        }
        try {
            FrameworkQueryConnector connector = resolveConnector(descriptor.connector());
            return executeConnector(connector, new QueryRequest<>(descriptor, input), outputType)
                .onItem().transformToUni(output -> captureFound(
                    store, executionContext, descriptor, captureKey, inputJson, output, outputType, Optional.empty()));
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <I, O> Uni<QueryOutcome<Object>> executeNative(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType
    ) {
        return executeNative(descriptor, input, outputType, Optional.empty());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <I, O> Uni<QueryOutcome<Object>> executeNative(
        QueryStepDescriptor descriptor,
        I input,
        Class<O> outputType,
        Optional<Function<O, ?>> localResultMapper
    ) {
        NativeQuerySelector selector = descriptor.nativeSelector().orElseThrow();
        ConnectorBindingRegistry bindings;
        try {
            bindings = requireBindingRegistry(selector);
        } catch (IllegalStateException failure) {
            return Uni.createFrom().failure(failure);
        }
        return Uni.createFrom().completionStage(bindings.activate(selector.binding(), runtimeContext))
            .onItem().transformToUni(ignored -> {
                try {
                    QueryOperation<?, ?, ?> operation = requireBoundQueryOperation(descriptor, selector);
                    ConnectorConfigurationDocument configuration =
                        new ConnectorConfigurationDocument(descriptor.config());
                    Optional<? extends ConnectorConfigSchema<?>> schema = operation.configurationSchema();
                    Object boundConfiguration = schema.isPresent()
                        ? ConnectorConfigurationBinder.bind(
                            schema.orElseThrow(), configuration, "native query operation " + selector.operationIdentity())
                        : zeroConfiguration(selector, configuration);
                    return invokeNative(
                        descriptor, selector, operation, input, boundConfiguration, outputType,
                        localResultMapper, bindings);
                } catch (RuntimeException failure) {
                    return Uni.createFrom().failure(failure);
                }
            });
    }

    private static ConnectorConfigurationDocument zeroConfiguration(
        NativeQuerySelector selector,
        ConnectorConfigurationDocument configuration
    ) {
        if (!configuration.values().isEmpty()) {
            throw new org.pipelineframework.connector.ConnectorConfigurationException(
                "native query operation " + selector.operationIdentity()
                    + " does not declare a configuration schema");
        }
        return ConnectorConfigurationDocument.empty();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <I, O> Multi<O> executeStreamingNative(
        QueryStepDescriptor descriptor,
        I input,
        Class<?> providerOutputType,
        Function<Object, O> rowMapper
    ) {
        NativeQuerySelector selector = descriptor.nativeSelector().orElseThrow();
        ConnectorBindingRegistry bindings;
        try {
            bindings = requireBindingRegistry(selector);
        } catch (IllegalStateException failure) {
            return Multi.createFrom().failure(failure);
        }
        return Uni.createFrom().completionStage(bindings.activate(selector.binding(), runtimeContext))
            .onItem().transformToMulti(ignored -> {
                try {
                    StreamingQueryOperation operation = requireBoundStreamingQueryOperation(descriptor, selector);
                    ConnectorConfigurationDocument configuration =
                        new ConnectorConfigurationDocument(descriptor.config());
                    Optional<? extends ConnectorConfigSchema<?>> schema = operation.configurationSchema();
                    Object boundConfiguration = schema.isPresent()
                        ? ConnectorConfigurationBinder.bind(
                            schema.orElseThrow(), configuration, "native streaming query operation " + selector.operationIdentity())
                        : zeroConfiguration(selector, configuration);
                    java.util.concurrent.Flow.Publisher<Object> publisher = invocationCoordinator.invokeStream(
                        selector.binding(),
                        operation,
                        () -> operation.query(new QueryInvocation<>(
                            input,
                            boundConfiguration,
                            providerOutputType,
                            connectorExecutionContext(descriptor),
                            Optional.of(bindings::materialize),
                            Optional.empty())));
                    return Multi.createFrom().publisher(publisher)
                        .onItem().transform(rowMapper)
                        .onFailure().transform(QueryStepSupport::unwrapTransportFailure);
                } catch (RuntimeException failure) {
                    return Multi.createFrom().failure(failure);
                }
            });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <I, O> Uni<QueryOutcome<Object>> invokeNative(
        QueryStepDescriptor descriptor,
        NativeQuerySelector selector,
        QueryOperation operation,
        I input,
        Object boundConfiguration,
        Class<O> outputType,
        Optional<Function<O, ?>> localResultMapper,
        ConnectorBindingRegistry bindings
    ) {
        CompletionStage<QueryOutcome<Object>> stage = invocationCoordinator.invoke(selector.binding(), operation, () ->
            operation.query(new QueryInvocation<>(
                input,
                boundConfiguration,
                outputType,
                connectorExecutionContext(descriptor),
                Optional.of(bindings::materialize),
                localResultMapper)));
        return Uni.createFrom().completionStage(stage)
            .onItem().invoke(outcome -> observeLive(selector, outcome))
            .onFailure().transform(QueryStepSupport::unwrapTransportFailure);
    }

    private void observeLive(NativeQuerySelector selector, QueryOutcome<Object> outcome) {
        if (outcome != null) {
            outcome.observation().ifPresent(observation ->
                observationTelemetry.record(selector.operationIdentity(), observation));
        }
    }

    private QueryOperation<?, ?, ?> requireBoundQueryOperation(
        QueryStepDescriptor descriptor,
        NativeQuerySelector selector
    ) {
        ConnectorBindingRegistry bindings = requireBindingRegistry(selector);
        var provider = bindings.requireProvider(selector.binding());
        if (!provider.id().equals(selector.operationIdentity().providerId())
            || provider.version().major() != selector.providerMajorVersion()) {
            throw new IllegalStateException(
                "connector binding '" + selector.binding().value() + "' resolves provider "
                    + provider.id().value() + " v" + provider.version().major() + " but Query descriptor requires "
                    + selector.operationIdentity().providerId().value() + " v" + selector.providerMajorVersion());
        }
        QueryOperation<?, ?, ?> operation = bindings.requireQueryOperation(
            selector.binding(),
            selector.operationIdentity().operationId(),
            selector.operationIdentity().majorVersion());
        if (!operation.capabilities().equals(descriptor.queryCapabilities().orElseThrow())) {
            throw new IllegalStateException(
                "query operation " + selector.operationIdentity()
                    + " runtime capabilities do not match its static manifest");
        }
        return operation;
    }

    private StreamingQueryOperation<?, ?, ?> requireBoundStreamingQueryOperation(
        QueryStepDescriptor descriptor,
        NativeQuerySelector selector
    ) {
        ConnectorBindingRegistry bindings = requireBindingRegistry(selector);
        var provider = bindings.requireProvider(selector.binding());
        if (!provider.id().equals(selector.operationIdentity().providerId())
            || provider.version().major() != selector.providerMajorVersion()) {
            throw new IllegalStateException(
                "connector binding '" + selector.binding().value() + "' resolves provider "
                    + provider.id().value() + " v" + provider.version().major()
                    + " but streaming Query descriptor requires "
                    + selector.operationIdentity().providerId().value() + " v" + selector.providerMajorVersion());
        }
        return bindings.requireStreamingQueryOperation(
            selector.binding(),
            selector.operationIdentity().operationId(),
            selector.operationIdentity().majorVersion());
    }

    private <O> Uni<O> applyNativeOutcome(
        QueryStepDescriptor descriptor,
        Class<O> outputType,
        QueryOutcome<Object> outcome,
        Optional<NativeCapture> capture
    ) {
        NativeQuerySelector selector = descriptor.nativeSelector().orElseThrow();
        if (outcome == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native query operation " + selector.operationIdentity() + " returned a null outcome"));
        }
        if (outcome instanceof QueryOutcome.Found<Object> found) {
            O output;
            try {
                output = outputType.cast(found.output());
            } catch (ClassCastException failure) {
                return Uni.createFrom().failure(new IllegalStateException(
                    "native query operation " + selector.operationIdentity() + " returned "
                        + found.output().getClass().getName() + " but step expected " + outputType.getName(), failure));
            }
            if (capture.isEmpty()) {
                return Uni.createFrom().item(output);
            }
            NativeCapture resolved = capture.orElseThrow();
            return captureFound(
                resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(), output,
                outputType, found.observation());
        }
        if (outcome instanceof QueryOutcome.NotFound<Object> notFound) {
            if (capture.isEmpty()) {
                return Uni.createFrom().failure(new QueryNotFoundException(notFound.code()));
            }
            NativeCapture resolved = capture.orElseThrow();
            return captureNotFound(
                resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(),
                notFound.code(), outputType, notFound.observation());
        }
        if (outcome instanceof QueryOutcome.TemporarilyUnavailable<Object> unavailable) {
            return Uni.createFrom().failure(new QueryTemporarilyUnavailableException(unavailable.code()));
        }
        if (outcome instanceof QueryOutcome.AuthenticationRequired<Object> authentication) {
            return Uni.createFrom().failure(new QueryAuthenticationRequiredException(authentication.code()));
        }
        if (outcome instanceof QueryOutcome.TerminalFailure<Object> terminal) {
            return Uni.createFrom().failure(new QueryTerminalFailureException(terminal.code()));
        }
        return Uni.createFrom().failure(new IllegalStateException(
            "unsupported query outcome from " + selector.operationIdentity() + ": " + outcome.getClass().getName()));
    }

    @SuppressWarnings("unchecked")
    private <O> Uni<QueryOutcome<O>> preserveNativeOutcome(
        QueryStepDescriptor descriptor,
        Class<O> outputType,
        QueryOutcome<Object> outcome,
        Optional<NativeCapture> capture
    ) {
        NativeQuerySelector selector = descriptor.nativeSelector().orElseThrow();
        if (outcome == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native query operation " + selector.operationIdentity() + " returned a null outcome"));
        }
        if (outcome instanceof QueryOutcome.Found<Object> found) {
            final O output;
            try {
                output = outputType.cast(found.output());
            } catch (ClassCastException failure) {
                return Uni.createFrom().failure(new IllegalStateException(
                    "native query operation " + selector.operationIdentity() + " returned "
                        + found.output().getClass().getName() + " but step expected " + outputType.getName(), failure));
            }
            if (capture.isEmpty()) {
                return Uni.createFrom().item(new QueryOutcome.Found<>(output, found.observation()));
            }
            NativeCapture resolved = capture.orElseThrow();
            return captureFound(
                    resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(), output,
                    outputType, found.observation())
                .replaceWith(new QueryOutcome.Found<>(output, found.observation()));
        }
        if (outcome instanceof QueryOutcome.NotFound<Object> notFound) {
            if (capture.isEmpty()) {
                return Uni.createFrom().item(new QueryOutcome.NotFound<>(notFound.code(), notFound.observation()));
            }
            NativeCapture resolved = capture.orElseThrow();
            return captureNotFoundRecord(
                    resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(),
                    notFound.code(), notFound.observation())
                .replaceWith(new QueryOutcome.NotFound<>(notFound.code(), notFound.observation()));
        }
        return Uni.createFrom().item((QueryOutcome<O>) outcome);
    }

    private ConnectorBindingRegistry requireBindingRegistry(NativeQuerySelector selector) {
        return bindingRegistry.orElseThrow(() -> new IllegalStateException(
            "connector binding registry is not available for Query operation " + selector.operationIdentity()));
    }

    private <O> Uni<O> executeConnector(
        FrameworkQueryConnector connector,
        QueryRequest<?> request,
        Class<O> outputType
    ) {
        CompletionStage<O> result;
        try {
            result = connector.queryOne(request, outputType);
        } catch (RuntimeException ex) {
            return Uni.createFrom().failure(ex);
        }
        if (result == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Framework query connector '" + connector.connectorName() + "' returned null CompletionStage"));
        }
        return Uni.createFrom().completionStage(result)
            .onItem().ifNull().failWith(() -> new IllegalStateException(
                "Framework query connector '" + connector.connectorName() + "' completed with null result"));
    }

    private Uni<Optional<QueryCaptureRecord>> getCaptured(QueryCaptureStore store, String captureKey) {
        CompletionStage<Optional<QueryCaptureRecord>> result;
        try {
            result = store.get(captureKey);
        } catch (RuntimeException ex) {
            return Uni.createFrom().failure(ex);
        }
        if (result == null) {
            return Uni.createFrom().failure(new QueryCaptureStoreException(
                "Query capture store '" + store.providerName() + "' returned null CompletionStage from get"));
        }
        return Uni.createFrom().completionStage(result)
            .onItem().ifNull().failWith(() -> new QueryCaptureStoreException(
                "Query capture store '" + store.providerName() + "' completed get with null Optional"));
    }

    private Uni<QueryCaptureRecord> putCaptured(QueryCaptureStore store, QueryCaptureRecord record) {
        CompletionStage<QueryCaptureRecord> result;
        try {
            result = store.putIfAbsent(record);
        } catch (RuntimeException ex) {
            return Uni.createFrom().failure(ex);
        }
        if (result == null) {
            return Uni.createFrom().failure(new QueryCaptureStoreException(
                "Query capture store '" + store.providerName() + "' returned null CompletionStage from putIfAbsent"));
        }
        return Uni.createFrom().completionStage(result)
            .onItem().ifNull().failWith(() -> new QueryCaptureStoreException(
                "Query capture store '" + store.providerName() + "' completed putIfAbsent with null record"));
    }

    private Uni<StreamingQueryCaptureOpen> openStreaming(
        QueryCaptureStore store,
        StreamingQueryCaptureRequest request
    ) {
        CompletionStage<StreamingQueryCaptureOpen> result;
        try {
            result = store.openStreaming(request);
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
        if (result == null) {
            return Uni.createFrom().failure(new QueryCaptureStoreException(
                "Query capture store '" + store.providerName() + "' returned null CompletionStage from openStreaming"));
        }
        return Uni.createFrom().completionStage(result)
            .onItem().ifNull().failWith(() -> new QueryCaptureStoreException(
                "Query capture store '" + store.providerName() + "' completed openStreaming with null result"));
    }

    private <O> Multi<O> captureStreaming(
        Multi<O> source,
        StreamingQueryCaptureWriter writer,
        Class<O> outputType
    ) {
        AtomicLong ordinal = new AtomicLong();
        Uni<Void> abort = writerStage(writer::abort, "abort").memoize().indefinitely();
        return source
            .onItem().call(output -> {
                try {
                    StreamingQueryCaptureItem item = new StreamingQueryCaptureItem(
                        ordinal.getAndIncrement(),
                        capturePayloadCodec.encode(output, outputType),
                        outputType.getName());
                    return writerStage(() -> writer.append(item), "append");
                } catch (Exception failure) {
                    return Uni.createFrom().failure(failure);
                }
            })
            .onCompletion().call(() -> writerStage(writer::commit, "commit"))
            .onFailure().call(ignored -> abort)
            .onCancellation().call(() -> abort);
    }

    private Uni<Void> writerStage(
        java.util.function.Supplier<CompletionStage<Void>> action,
        String operation
    ) {
        return Uni.createFrom().deferred(() -> {
            CompletionStage<Void> stage;
            try {
                stage = action.get();
            } catch (RuntimeException failure) {
                return Uni.createFrom().failure(failure);
            }
            if (stage == null) {
                return Uni.createFrom().failure(new IllegalStateException(
                    "streaming Query capture writer returned null CompletionStage from " + operation));
            }
            return Uni.createFrom().completionStage(stage);
        });
    }

    private <O> O decodeStreamingCapture(StreamingQueryCaptureItem item, Class<O> outputType) {
        if (!outputType.getName().equals(item.outputType())) {
            throw new QueryCaptureStoreException(
                "Streaming Query capture item " + item.ordinal() + " has type " + item.outputType()
                    + " but step expected " + outputType.getName());
        }
        try {
            return capturePayloadCodec.decode(item.outputJson(), outputType);
        } catch (Exception failure) {
            throw new QueryCaptureStoreException(
                "Streaming Query capture item " + item.ordinal()
                    + " cannot be read as " + outputType.getName(), failure);
        }
    }

    private FrameworkQueryConnector resolveConnector(String connectorName) {
        List<FrameworkQueryConnector> matches = connectors.stream()
            .filter(connector -> connectorName.equals(connector.connectorName()))
            .toList();
        if (matches.isEmpty()) {
            throw new IllegalStateException("No framework query connector registered with connectorName '" + connectorName + "'");
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple framework query connectors registered with connectorName '" + connectorName + "'");
        }
        return matches.get(0);
    }

    private QueryCaptureStore resolveStore() {
        if (stores.isEmpty()) {
            throw new QueryCaptureStoreException("No QueryCaptureStore is registered");
        }
        if (stores.size() > 1) {
            throw new QueryCaptureStoreException(
                "Multiple QueryCaptureStore beans are registered: "
                    + stores.stream().map(QueryCaptureStore::providerName).sorted().toList());
        }
        return stores.getFirst();
    }

    private <O> Uni<O> captureFound(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        QueryStepDescriptor descriptor,
        String captureKey,
        String inputJson,
        O output,
        Class<O> outputType,
        Optional<QueryObservation> observation
    ) {
        try {
            String outputJson = capturePayloadCodec.encode(output, outputType);
            QueryCaptureRecord record = new QueryCaptureRecord(
                context.tenantId(),
                context.executionId(),
                context.currentStepIndex(),
                descriptor.queryId(),
                descriptor.version(),
                captureKey,
                inputJson,
                outputJson,
                outputType.getName(),
                Instant.now(),
                QueryCaptureStatus.FOUND,
                "found",
                observation);
            return putCaptured(store, record).onItem().transformToUni(captured -> decodeCaptured(captured, outputType));
        } catch (Exception ex) {
            return Uni.createFrom().failure(ex);
        }
    }

    private <O> Uni<O> captureNotFound(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        QueryStepDescriptor descriptor,
        String captureKey,
        String inputJson,
        String outcomeCode,
        Class<O> outputType,
        Optional<QueryObservation> observation
    ) {
        return captureNotFoundRecord(store, context, descriptor, captureKey, inputJson, outcomeCode, observation)
            .onItem().transformToUni(captured -> decodeCaptured(captured, outputType));
    }

    private Uni<QueryCaptureRecord> captureNotFoundRecord(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        QueryStepDescriptor descriptor,
        String captureKey,
        String inputJson,
        String outcomeCode,
        Optional<QueryObservation> observation
    ) {
        QueryCaptureRecord record = new QueryCaptureRecord(
            context.tenantId(),
            context.executionId(),
            context.currentStepIndex(),
            descriptor.queryId(),
            descriptor.version(),
            captureKey,
            inputJson,
            "",
            "",
            Instant.now(),
            QueryCaptureStatus.NOT_FOUND,
            outcomeCode,
            observation);
        return putCaptured(store, record);
    }

    private <O> Uni<QueryOutcome<O>> replayCapturedOutcome(
        QueryStepDescriptor descriptor,
        QueryCaptureRecord record,
        Class<O> outputType
    ) {
        Optional<QueryObservation> observation = recordReplay(descriptor, record);
        if (record.status() == QueryCaptureStatus.NOT_FOUND) {
            return Uni.createFrom().item(new QueryOutcome.NotFound<>(record.outcomeCode(), observation));
        }
        return decodeCaptured(record, outputType)
            .onItem().transform(output -> new QueryOutcome.Found<>(output, observation));
    }

    private <O> Uni<O> replayCaptured(
        QueryStepDescriptor descriptor,
        QueryCaptureRecord record,
        Class<O> outputType
    ) {
        recordReplay(descriptor, record);
        return decodeCaptured(record, outputType);
    }

    private Optional<QueryObservation> recordReplay(QueryStepDescriptor descriptor, QueryCaptureRecord record) {
        Optional<QueryObservation> replayed = record.observation().map(QueryObservation::asReplay);
        replayed.ifPresent(observation -> observationTelemetry.record(
            descriptor.nativeSelector().orElseThrow().operationIdentity(), observation));
        return replayed;
    }

    private <O> Uni<O> decodeCaptured(QueryCaptureRecord record, Class<O> outputType) {
        if (record.status() == QueryCaptureStatus.NOT_FOUND) {
            return Uni.createFrom().failure(new QueryNotFoundException(record.outcomeCode()));
        }
        if (!outputType.getName().equals(record.outputType())) {
            return Uni.createFrom().failure(new QueryCaptureStoreException(
                "Captured query output for key '" + record.captureKey()
                    + "' has type " + record.outputType()
                    + " but step expected " + outputType.getName()));
        }
        try {
            return Uni.createFrom().item(capturePayloadCodec.decode(record.outputJson(), outputType));
        } catch (Exception ex) {
            return Uni.createFrom().failure(new QueryCaptureStoreException(
                "Captured query output for key '" + record.captureKey()
                    + "' cannot be read as " + outputType.getName(), ex));
        }
    }

    private Object normalizedKeyInput(Object input, List<String> keyFields) {
        if (keyFields == null || keyFields.isEmpty()) {
            return json.valueToTree(input);
        }
        JsonNode root = json.valueToTree(input);
        com.fasterxml.jackson.databind.node.ObjectNode node = json.createObjectNode();
        for (String field : keyFields) {
            JsonNode value = root.path(field);
            node.set(field, value.isMissingNode() ? json.nullNode() : value);
        }
        return node;
    }

    private String captureKey(PipelineExecutionContext context, QueryStepDescriptor descriptor, String inputJson) {
        String basis = context.tenantId()
            + ":" + context.executionId()
            + ":" + context.currentStepIndex()
            + ":" + descriptor.queryId()
            + ":" + descriptor.version()
            + ":" + inputJson;
        String tenant = Base64.getUrlEncoder().withoutPadding().encodeToString(
            context.tenantId().getBytes(StandardCharsets.UTF_8));
        return tenant + "." + HexFormat.of().formatHex(sha256(basis));
    }

    private byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            throw new IllegalStateException("SHA-256 digest is unavailable", ex);
        }
    }

    private static ConnectorExecutionContext connectorExecutionContext(QueryStepDescriptor descriptor) {
        Optional<PipelineExecutionContext> context = PipelineExecutionContextHolder.get();
        return new ConnectorExecutionContext(
            context.map(PipelineExecutionContext::tenantId),
            context.map(PipelineExecutionContext::executionId),
            Optional.of(descriptor.stepId()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    }

    private static Throwable unwrapTransportFailure(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> List<T> toList(Instance<T> instance) {
        if (instance == null || instance.isUnsatisfied()) {
            return List.of();
        }
        List<T> items = new ArrayList<>();
        for (T item : instance) {
            items.add(item);
        }
        return List.copyOf(items);
    }

    private record NativeCapture(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        String captureKey,
        String inputJson
    ) {
    }
}
