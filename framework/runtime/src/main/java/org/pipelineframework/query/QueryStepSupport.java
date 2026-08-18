package org.pipelineframework.query;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationBinder;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.ConnectorRuntimeContext;

/**
 * Runtime support for generated captured query client steps.
 */
@ApplicationScoped
public class QueryStepSupport {
    private final List<FrameworkQueryConnector> connectors;
    private final List<QueryCaptureStore> stores;
    private final Optional<ConnectorBindingRegistry> bindingRegistry;
    private final ConnectorRuntimeContext runtimeContext;
    private final ObjectMapper json = PipelineJson.mapper();
    private final QueryCapturePayloadCodec capturePayloadCodec = new QueryCapturePayloadCodec(json);

    @Inject
    public QueryStepSupport(
        Instance<FrameworkQueryConnector> connectors,
        Instance<QueryCaptureStore> stores,
        ConnectorBindingRegistry bindingRegistry,
        ConnectorRuntimeContext runtimeContext
    ) {
        this(toList(connectors), toStores(stores), Optional.of(bindingRegistry), runtimeContext);
    }

    public QueryStepSupport(Collection<FrameworkQueryConnector> connectors, Collection<QueryCaptureStore> stores) {
        this(connectors, stores, Optional.empty(), ConnectorRuntimeContext.empty());
    }

    public QueryStepSupport(
        Collection<FrameworkQueryConnector> connectors,
        Collection<QueryCaptureStore> stores,
        ConnectorBindingRegistry bindingRegistry
    ) {
        this(connectors, stores, Optional.ofNullable(bindingRegistry), ConnectorRuntimeContext.empty());
    }

    public QueryStepSupport(
        Collection<FrameworkQueryConnector> connectors,
        Collection<QueryCaptureStore> stores,
        ConnectorBindingRegistry bindingRegistry,
        ConnectorRuntimeContext runtimeContext
    ) {
        this(connectors, stores, Optional.ofNullable(bindingRegistry), runtimeContext);
    }

    private QueryStepSupport(
        Collection<FrameworkQueryConnector> connectors,
        Collection<QueryCaptureStore> stores,
        Optional<ConnectorBindingRegistry> bindingRegistry,
        ConnectorRuntimeContext runtimeContext
    ) {
        this.connectors = connectors == null ? List.of() : List.copyOf(connectors);
        this.stores = stores == null || stores.isEmpty()
            ? List.of(new InMemoryQueryCaptureStore())
            : List.copyOf(stores);
        this.bindingRegistry = java.util.Objects.requireNonNull(
            bindingRegistry, "connector binding registry selection must not be null");
        this.runtimeContext = java.util.Objects.requireNonNull(runtimeContext, "connector runtime context must not be null");
    }

    public <I, O> Uni<O> queryOneToOne(Uni<QueryStepDescriptor> descriptor, I input, Class<O> outputType) {
        return descriptor.onItem().transformToUni(resolved -> queryOneToOne(resolved, input, outputType));
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
                    return replayCaptured(existing.get(), outputType);
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
                    return replayCapturedOutcome(existing.orElseThrow(), outputType);
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
                    store, executionContext, descriptor, captureKey, inputJson, output, outputType));
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
                        descriptor, selector, operation, input, boundConfiguration, outputType, bindings);
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
    private <I> Uni<QueryOutcome<Object>> invokeNative(
        QueryStepDescriptor descriptor,
        NativeQuerySelector selector,
        QueryOperation operation,
        I input,
        Object boundConfiguration,
        Class<?> outputType,
        ConnectorBindingRegistry bindings
    ) {
        CompletionStage<QueryOutcome<Object>> stage;
        try {
            stage = operation.query(new QueryInvocation<>(
                input,
                boundConfiguration,
                outputType,
                connectorExecutionContext(descriptor),
                Optional.of(bindings::materialize)));
        } catch (Throwable failure) {
            return Uni.createFrom().failure(unwrapTransportFailure(failure));
        }
        if (stage == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native query operation " + selector.operationIdentity() + " returned a null CompletionStage"));
        }
        return Uni.createFrom().completionStage(stage)
            .onFailure().transform(QueryStepSupport::unwrapTransportFailure);
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
                resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(), output, outputType);
        }
        if (outcome instanceof QueryOutcome.NotFound<Object> notFound) {
            if (capture.isEmpty()) {
                return Uni.createFrom().failure(new QueryNotFoundException(notFound.code()));
            }
            NativeCapture resolved = capture.orElseThrow();
            return captureNotFound(
                resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(),
                notFound.code(), outputType);
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
                return Uni.createFrom().item(new QueryOutcome.Found<>(output));
            }
            NativeCapture resolved = capture.orElseThrow();
            return captureFound(
                    resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(), output, outputType)
                .replaceWith(new QueryOutcome.Found<>(output));
        }
        if (outcome instanceof QueryOutcome.NotFound<Object> notFound) {
            if (capture.isEmpty()) {
                return Uni.createFrom().item(new QueryOutcome.NotFound<>(notFound.code()));
            }
            NativeCapture resolved = capture.orElseThrow();
            return captureNotFoundRecord(
                    resolved.store(), resolved.context(), descriptor, resolved.captureKey(), resolved.inputJson(), notFound.code())
                .replaceWith(new QueryOutcome.NotFound<>(notFound.code()));
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
            return Uni.createFrom().failure(new IllegalStateException(
                "Query capture store '" + store.providerName() + "' returned null CompletionStage from get"));
        }
        return Uni.createFrom().completionStage(result)
            .onItem().ifNull().failWith(() -> new IllegalStateException(
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
            return Uni.createFrom().failure(new IllegalStateException(
                "Query capture store '" + store.providerName() + "' returned null CompletionStage from putIfAbsent"));
        }
        return Uni.createFrom().completionStage(result)
            .onItem().ifNull().failWith(() -> new IllegalStateException(
                "Query capture store '" + store.providerName() + "' completed putIfAbsent with null record"));
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
        // V1 captures query results in the in-memory store. A durable store/provider selector should
        // be added when query capture graduates beyond the initial runtime primitive.
        List<QueryCaptureStore> matches = stores.stream()
            .filter(store -> "memory".equals(store.providerName()))
            .toList();
        if (matches.isEmpty()) {
            throw new IllegalStateException("No QueryCaptureStore registered with providerName 'memory'");
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple QueryCaptureStore beans registered with providerName 'memory'");
        }
        return matches.get(0);
    }

    private <O> Uni<O> captureFound(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        QueryStepDescriptor descriptor,
        String captureKey,
        String inputJson,
        O output,
        Class<O> outputType
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
                "found");
            return putCaptured(store, record).onItem().transformToUni(captured -> replayCaptured(captured, outputType));
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
        Class<O> outputType
    ) {
        return captureNotFoundRecord(store, context, descriptor, captureKey, inputJson, outcomeCode)
            .onItem().transformToUni(captured -> replayCaptured(captured, outputType));
    }

    private Uni<QueryCaptureRecord> captureNotFoundRecord(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        QueryStepDescriptor descriptor,
        String captureKey,
        String inputJson,
        String outcomeCode
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
            outcomeCode);
        return putCaptured(store, record);
    }

    private <O> Uni<QueryOutcome<O>> replayCapturedOutcome(QueryCaptureRecord record, Class<O> outputType) {
        if (record.status() == QueryCaptureStatus.NOT_FOUND) {
            return Uni.createFrom().item(new QueryOutcome.NotFound<>(record.outcomeCode()));
        }
        return replayCaptured(record, outputType).onItem().transform(output -> new QueryOutcome.Found<>(output));
    }

    private <O> Uni<O> replayCaptured(QueryCaptureRecord record, Class<O> outputType) {
        if (record.status() == QueryCaptureStatus.NOT_FOUND) {
            return Uni.createFrom().failure(new QueryNotFoundException(record.outcomeCode()));
        }
        if (!outputType.getName().equals(record.outputType())) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Captured query output for key '" + record.captureKey()
                    + "' has type " + record.outputType()
                    + " but step expected " + outputType.getName()));
        }
        try {
            return Uni.createFrom().item(capturePayloadCodec.decode(record.outputJson(), outputType));
        } catch (Exception ex) {
            return Uni.createFrom().failure(new IllegalStateException(
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
        return HexFormat.of().formatHex(sha256(basis));
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

    private static List<QueryCaptureStore> toStores(Instance<QueryCaptureStore> stores) {
        List<QueryCaptureStore> resolved = toList(stores);
        return resolved.isEmpty() ? List.of(new InMemoryQueryCaptureStore()) : resolved;
    }

    private record NativeCapture(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        String captureKey,
        String inputJson
    ) {
    }
}
