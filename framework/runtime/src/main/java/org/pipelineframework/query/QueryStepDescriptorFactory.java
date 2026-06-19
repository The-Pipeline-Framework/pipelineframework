package org.pipelineframework.query;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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

    public Uni<QueryStepDescriptor> descriptor(String serviceName, String inputType, String outputType) {
        String cacheKey = descriptorCacheKey(serviceName, inputType, outputType);
        QueryStepDescriptor cached = descriptors.get(cacheKey);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }
        return Uni.createFrom()
            .item(() -> descriptors.computeIfAbsent(cacheKey, key -> loadDescriptor(serviceName, inputType, outputType)))
            .runSubscriptionOn(blockingExecutor);
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
        Path base = resolveConfigBase();
        Path configPath = new PipelineYamlConfigLocator().locate(base)
            .orElseThrow(() -> new IllegalStateException("No pipeline YAML found for query step " + serviceName));
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        PipelineYamlStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No query YAML step found for generated service " + serviceName));
        if (!"query".equalsIgnoreCase(step.kind())) {
            throw new IllegalStateException("Generated query service " + serviceName + " maps to non-query YAML step");
        }
        if (step.queryId() == null || step.queryId().isBlank()) {
            throw new IllegalStateException("Query step " + serviceName + " is missing query");
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
            query.config());
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

    private static Path resolveConfigBase() {
        String explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit != null) {
            Path candidate = Path.of(explicit);
            if (candidate.isAbsolute()) {
                return candidate.getParent() != null ? candidate.getParent() : candidate;
            }
        }
        return Path.of("").toAbsolutePath();
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
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
}
