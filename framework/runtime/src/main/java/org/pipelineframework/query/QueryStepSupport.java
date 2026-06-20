package org.pipelineframework.query;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

/**
 * Runtime support for generated captured query client steps.
 */
@ApplicationScoped
public class QueryStepSupport {
    private final List<QueryConnector<?, ?>> connectors;
    private final List<QueryCaptureStore> stores;
    private final ObjectMapper json = PipelineJson.mapper();

    @Inject
    public QueryStepSupport(Instance<QueryConnector<?, ?>> connectors, Instance<QueryCaptureStore> stores) {
        this(toList(connectors), toStores(stores));
    }

    public QueryStepSupport(Collection<QueryConnector<?, ?>> connectors, Collection<QueryCaptureStore> stores) {
        this.connectors = connectors == null ? List.of() : List.copyOf(connectors);
        this.stores = stores == null || stores.isEmpty()
            ? List.of(new InMemoryQueryCaptureStore())
            : List.copyOf(stores);
    }

    public <I, O> Uni<O> queryOneToOne(Uni<QueryStepDescriptor> descriptor, I input, Class<O> outputType) {
        return descriptor.onItem().transformToUni(resolved -> queryOneToOne(resolved, input, outputType));
    }

    public <I, O> Uni<O> queryOneToOne(QueryStepDescriptor descriptor, I input, Class<O> outputType) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        QueryConnector<I, O> connector;
        try {
            connector = resolveConnector(descriptor.connector());
        } catch (RuntimeException ex) {
            return Uni.createFrom().failure(ex);
        }
        Optional<PipelineExecutionContext> context = PipelineExecutionContextHolder.get();
        if (context.isEmpty()) {
            return connector.execute(new QueryRequest<>(descriptor, input, descriptor.config()));
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
        return store.get(captureKey)
            .onItem().transformToUni(existing -> {
                if (existing.isPresent()) {
                    return coerce(existing.get(), outputType);
                }
                return connector.execute(new QueryRequest<>(descriptor, input, descriptor.config()))
                    .onItem().transformToUni(output -> capture(store, executionContext, descriptor, captureKey, inputJson, output, outputType));
            });
    }

    @SuppressWarnings("unchecked")
    private <I, O> QueryConnector<I, O> resolveConnector(String connectorName) {
        List<QueryConnector<?, ?>> matches = connectors.stream()
            .filter(connector -> connectorName.equals(connector.connectorName()))
            .toList();
        if (matches.isEmpty()) {
            throw new IllegalStateException("No QueryConnector registered with connectorName '" + connectorName + "'");
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Multiple QueryConnector beans registered with connectorName '" + connectorName + "'");
        }
        return (QueryConnector<I, O>) matches.get(0);
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

    private <O> Uni<O> capture(
        QueryCaptureStore store,
        PipelineExecutionContext context,
        QueryStepDescriptor descriptor,
        String captureKey,
        String inputJson,
        O output,
        Class<O> outputType
    ) {
        try {
            String outputJson = json.writeValueAsString(output);
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
                Instant.now());
            return store.putIfAbsent(record).onItem().transformToUni(captured -> coerce(captured, outputType));
        } catch (Exception ex) {
            return Uni.createFrom().failure(ex);
        }
    }

    private <O> Uni<O> coerce(QueryCaptureRecord record, Class<O> outputType) {
        if (!outputType.getName().equals(record.outputType())) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Captured query output for key '" + record.captureKey()
                    + "' has type " + record.outputType()
                    + " but step expected " + outputType.getName()));
        }
        try {
            return Uni.createFrom().item(json.readValue(record.outputJson(), outputType));
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
}
