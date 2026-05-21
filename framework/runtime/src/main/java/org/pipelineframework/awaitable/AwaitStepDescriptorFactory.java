package org.pipelineframework.awaitable;

import java.nio.file.Path;
import java.time.Duration;
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
import org.pipelineframework.config.pipeline.PipelineYamlStep;

/**
 * Builds await descriptors from runtime pipeline YAML.
 */
@ApplicationScoped
public class AwaitStepDescriptorFactory {
    private static final int DESCRIPTOR_LOADER_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final int DESCRIPTOR_LOADER_QUEUE_SIZE = 256;

    private final Map<String, AwaitStepDescriptor> descriptors = new ConcurrentHashMap<>();
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
            t.setName("await-descriptor-loader-" + threadCounter.incrementAndGet());
            return t;
        });

    /**
     * Resolves the descriptor for a generated await step.
     */
    public Uni<AwaitStepDescriptor> descriptor(String serviceName, String inputType, String outputType) {
        AwaitStepDescriptor cached = descriptors.get(serviceName);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }
        return Uni.createFrom()
            .item(() -> descriptors.computeIfAbsent(serviceName, key -> loadDescriptor(key, inputType, outputType)))
            .runSubscriptionOn(blockingExecutor);
    }

    @PreDestroy
    void shutdown() {
        blockingExecutor.shutdown();
    }

    private AwaitStepDescriptor loadDescriptor(String serviceName, String inputType, String outputType) {
        Path base = Path.of("").toAbsolutePath();
        Path configPath = new PipelineYamlConfigLocator().locate(base)
            .orElseThrow(() -> new IllegalStateException("No pipeline YAML found for await step " + serviceName));
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        PipelineYamlStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No await YAML step found for generated service " + serviceName));
        if (!"await".equalsIgnoreCase(step.kind())) {
            throw new IllegalStateException("Generated await service " + serviceName + " maps to non-await YAML step");
        }
        if (step.awaitConfig() == null) {
            throw new IllegalStateException("Await step " + serviceName + " is missing await configuration");
        }
        if (step.awaitConfig().transport() == null) {
            throw new IllegalStateException("Await step " + serviceName + " is missing await.transport configuration");
        }
        if (step.awaitConfig().correlation() == null) {
            throw new IllegalStateException("Await step " + serviceName + " is missing await.correlation configuration");
        }
        if (step.awaitConfig().correlation().strategy() == null || step.awaitConfig().correlation().strategy().isBlank()) {
            throw new IllegalArgumentException("Await step " + serviceName + " is missing await.correlation.strategy");
        }
        if (step.timeout() == null || step.timeout().isBlank()) {
            throw new IllegalArgumentException("Await step " + serviceName + " is missing timeout");
        }
        Duration timeout;
        try {
            timeout = Duration.parse(step.timeout());
        } catch (java.time.format.DateTimeParseException ex) {
            throw new IllegalArgumentException("Await step " + serviceName + " has invalid timeout format: " + step.timeout(), ex);
        }
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            serviceName,
            inputType,
            outputType,
            step.cardinality(),
            step.awaitConfig().dispatch().mode(),
            timeout,
            step.awaitConfig().correlation().strategy(),
            step.awaitConfig().transport().type(),
            step.awaitConfig().transport().config(),
            step.idempotencyKeyFields());
        return descriptor;
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
