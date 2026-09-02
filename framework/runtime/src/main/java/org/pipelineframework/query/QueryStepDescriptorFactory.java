package org.pipelineframework.query;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlQuery;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Builds query descriptors from runtime pipeline YAML.
 */
@ApplicationScoped
public class QueryStepDescriptorFactory {
    private static final int DESCRIPTOR_LOADER_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final int DESCRIPTOR_LOADER_QUEUE_SIZE = 256;

    private final Map<String, QueryStepDescriptor> descriptors = new ConcurrentHashMap<>();
    private final AtomicInteger threadCounter = new AtomicInteger();
    private final ExecutorService blockingExecutor = new ThreadPoolExecutor(
        1,
        DESCRIPTOR_LOADER_THREADS,
        60L,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(DESCRIPTOR_LOADER_QUEUE_SIZE),
        r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("query-descriptor-loader-" + threadCounter.incrementAndGet());
            return t;
        });
    private final Executor contextualBlockingExecutor = task -> {
        Callable<Void> contextualized = RuntimeAdapters.executionContextCarrier().contextualize(() -> {
            task.run();
            return null;
        });
        blockingExecutor.execute(() -> call(contextualized));
    };

    public Uni<QueryStepDescriptor> descriptor(String serviceName, String inputType, String outputType) {
        String cacheKey = descriptorCacheKey(serviceName, inputType, outputType);
        QueryStepDescriptor cached = descriptors.get(cacheKey);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }
        return Uni.createFrom()
            .item(() -> descriptors.computeIfAbsent(cacheKey, key -> loadDescriptor(serviceName, inputType, outputType)))
            .runSubscriptionOn(contextualBlockingExecutor);
    }

    private static <T> T call(Callable<T> task) {
        try {
            return task.call();
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("Query descriptor loading failed", failure);
        }
    }

    @PreDestroy
    void shutdown() {
        blockingExecutor.shutdown();
        try {
            if (!blockingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                blockingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            blockingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private QueryStepDescriptor loadDescriptor(String serviceName, String inputType, String outputType) {
        PipelineYamlConfig config = loadPipelineConfig(serviceName);
        PipelineYamlStep step = config.stepDefinitions().values().stream()
            .flatMap(java.util.Collection::stream)
            .filter(candidate -> matchesServiceName(serviceName, candidate.name()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No query YAML step found for generated service " + serviceName));
        if (!"query".equalsIgnoreCase(step.kind())) {
            throw new IllegalStateException("Generated query service " + serviceName + " maps to non-query YAML step");
        }
        if (step.operationSelection().isPresent()) {
            org.pipelineframework.config.pipeline.PipelineYamlOperationSelection selectedOperation =
                step.operationSelection().orElseThrow();
            org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding binding =
                config.connectors().get(selectedOperation.using());
            if (binding == null) {
                throw new IllegalStateException("Query step " + serviceName + " references unknown connector binding '"
                    + selectedOperation.using() + "'");
            }
            ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
                ConnectorProviderId.of(binding.provider()),
                selectedOperation.operation(),
                ConnectorOperationKind.QUERY,
                selectedOperation.operationVersion());
            org.pipelineframework.connector.QueryOperationCardinality cardinality =
                providerManifestCatalog().requireQueryCardinality(identity, binding.version());
            String declaredCardinality = step.cardinality() == null || step.cardinality().isBlank()
                ? "ONE_TO_ONE"
                : step.cardinality().strip().toUpperCase(java.util.Locale.ROOT);
            if (!cardinality.name().equals(declaredCardinality)) {
                throw new IllegalStateException("Query step " + serviceName + " cardinality "
                    + declaredCardinality + " does not match provider operation cardinality " + cardinality);
            }
            if (cardinality == org.pipelineframework.connector.QueryOperationCardinality.ONE_TO_MANY) {
                if (step.negativeCacheTtl().isPresent()) {
                    throw new IllegalStateException("Streaming Query step " + serviceName
                        + " must not declare negative cache TTL");
                }
                return QueryStepDescriptor.nativeStreamingQuery(
                    serviceName,
                    inputType,
                    outputType,
                    new NativeQuerySelector(
                        ConnectorBindingName.of(binding.name()),
                        identity,
                        binding.version()),
                    step.operationConfig(),
                    step.queryCapture() == null || step.queryCapture().keyFields() == null
                        ? java.util.List.of()
                        : step.queryCapture().keyFields());
            }
            QueryCapabilities capabilities = providerManifestCatalog().requireQueryCapabilities(identity, binding.version());
            return QueryStepDescriptor.nativeQuery(
                serviceName,
                inputType,
                outputType,
                declaredCardinality,
                new NativeQuerySelector(
                    ConnectorBindingName.of(binding.name()),
                    identity,
                    binding.version()),
                step.operationConfig(),
                step.queryCapture() == null || step.queryCapture().keyFields() == null
                    ? java.util.List.of()
                    : step.queryCapture().keyFields(),
                capabilities,
                step.negativeCacheTtl());
        }
        if (step.queryId() == null || step.queryId().isBlank()) {
            throw new IllegalStateException("Query step " + serviceName + " is missing query");
        }
        if ("ONE_TO_MANY".equalsIgnoreCase(step.cardinality())) {
            throw new IllegalStateException("Streaming Query step " + serviceName
                + " requires a native operation/using selection");
        }
        PipelineYamlQuery query = config.queries().get(step.queryId());
        if (query == null) {
            throw new IllegalStateException("Query step " + serviceName + " references unknown query '" + step.queryId() + "'");
        }
        if (!sameType(inputType, query.inputType()) || !sameType(outputType, query.outputType())) {
            throw new IllegalStateException("Query step " + serviceName + " type mismatch: step ["
                + inputType + " -> " + outputType + "] query ["
                + query.inputType() + " -> " + query.outputType() + "]");
        }
        return new QueryStepDescriptor(
            serviceName,
            step.queryId(),
            query.connector(),
            query.version(),
            inputType,
            outputType,
            step.cardinality(),
            step.queryCapture() == null || step.queryCapture().keyFields() == null
                ? java.util.List.of()
                : step.queryCapture().keyFields(),
            query.jpa());
    }

    private static String descriptorCacheKey(String serviceName, String inputType, String outputType) {
        return String.join(
            "\u001f",
            serviceName == null ? "" : serviceName,
            inputType == null ? "" : inputType,
            outputType == null ? "" : outputType);
    }

    private static boolean sameType(String stepType, String queryType) {
        if (stepType == null || queryType == null) {
            return false;
        }
        if (stepType.equals(queryType) || stepType.endsWith("." + queryType) || queryType.endsWith("." + stepType)) {
            return true;
        }
        String stepSimple = simpleTypeName(stepType);
        String querySimple = simpleTypeName(queryType);
        return stepSimple.equals(querySimple)
            || stepSimple.equals(querySimple + "Dto")
            || querySimple.equals(stepSimple + "Dto");
    }

    private static String simpleTypeName(String type) {
        if (type == null || type.isBlank()) {
            return "";
        }
        int lastDot = type.lastIndexOf('.');
        return lastDot >= 0 ? type.substring(lastDot + 1) : type;
    }

    private static PipelineYamlConfig loadPipelineConfig(String serviceName) {
        PipelineYamlConfigLoader loader = new PipelineYamlConfigLoader();
        Optional<String> explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit.isPresent()) {
            Path candidate = Path.of(explicit.get()).toAbsolutePath().normalize();
            if (!Files.isDirectory(candidate)) {
                return loader.load(candidate);
            }
            Optional<Path> located = new PipelineYamlConfigLocator().locate(candidate);
            if (located.isPresent()) {
                return loader.load(located.orElseThrow());
            }
        } else {
            Optional<Path> located = new PipelineYamlConfigLocator().locate(Path.of("").toAbsolutePath());
            if (located.isPresent()) {
                return loader.load(located.orElseThrow());
            }
        }

        ClassLoader context = Thread.currentThread().getContextClassLoader();
        ClassLoader fallback = QueryStepDescriptorFactory.class.getClassLoader();
        Optional<URL> resource = java.util.stream.Stream.of(context, fallback)
            .filter(Objects::nonNull)
            .distinct()
            .map(classLoader -> new PipelineYamlConfigLocator().locateResource(classLoader))
            .flatMap(Optional::stream)
            .findFirst();
        if (resource.isPresent()) {
            try (InputStream stream = resource.orElseThrow().openStream()) {
                return loader.load(stream);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read pipeline YAML for query step " + serviceName, e);
            }
        }
        throw new IllegalStateException("No pipeline YAML found for query step " + serviceName);
    }

    private static Optional<String> firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    private static ConnectorProviderManifestCatalog providerManifestCatalog() {
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        return ConnectorProviderManifestLoader.load(
            context == null ? QueryStepDescriptorFactory.class.getClassLoader() : context);
    }

    private static String toServiceName(String stepName) {
        if (stepName == null || stepName.isBlank()) {
            return "ProcessStepService";
        }
        String stripped = stepName.startsWith("Process ") ? stepName.substring("Process ".length()) : stepName;
        StringBuilder formatted = new StringBuilder();
        for (String part : stripped.split(" ")) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(java.util.Locale.ROOT);
            formatted.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                formatted.append(lower.substring(1));
            }
        }
        return formatted.isEmpty() ? "ProcessStepService" : "Process" + formatted + "Service";
    }

    private static boolean matchesServiceName(String generatedServiceName, String stepName) {
        if (generatedServiceName == null || generatedServiceName.isBlank()) {
            return false;
        }
        return generatedServiceName.equals(toServiceName(stepName))
            || generatedServiceName.equals(toCompactServiceName(stepName));
    }

    private static String toCompactServiceName(String stepName) {
        String serviceName = toServiceName(stepName);
        if (serviceName.startsWith("Process") && serviceName.endsWith("Service")) {
            return serviceName.substring("Process".length(), serviceName.length() - "Service".length());
        }
        return serviceName;
    }
}
