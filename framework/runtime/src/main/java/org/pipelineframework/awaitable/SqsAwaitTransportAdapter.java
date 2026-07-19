package org.pipelineframework.awaitable;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.function.Supplier;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.config.ConfigProvider;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.sqs.SqsAwaitDispatchEnvelope;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Await transport adapter that publishes one interaction request to SQS.
 */
@ApplicationScoped
public class SqsAwaitTransportAdapter implements AwaitTransportAdapter<Object> {

    @Inject
    AwaitResumeTokenService resumeTokenService;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private volatile SqsClient client;
    private final SqsClient explicitClient;
    private final Supplier<SqsLiveAwaitWindowConfig> liveAwaitWindowConfig;

    public SqsAwaitTransportAdapter() {
        this.explicitClient = null;
        this.liveAwaitWindowConfig = SqsLiveAwaitWindowConfig::fromRuntime;
    }

    SqsAwaitTransportAdapter(SqsClient explicitClient, PipelineOrchestratorConfig orchestratorConfig) {
        this(explicitClient, orchestratorConfig, SqsLiveAwaitWindowConfig::fromRuntime);
    }

    SqsAwaitTransportAdapter(
        SqsClient explicitClient,
        PipelineOrchestratorConfig orchestratorConfig,
        Supplier<SqsLiveAwaitWindowConfig> liveAwaitWindowConfig
    ) {
        this.explicitClient = explicitClient;
        this.client = explicitClient;
        this.orchestratorConfig = orchestratorConfig;
        this.liveAwaitWindowConfig = Objects.requireNonNull(liveAwaitWindowConfig, "liveAwaitWindowConfig must not be null");
    }

    @Override
    public String type() {
        return "sqs";
    }

    @Override
    public boolean supportsLiveAwaitWindow(AwaitStepDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        return liveAwaitWindowConfig.get().matches(SqsConfig.from(descriptor.transportConfig()));
    }

    @Override
    public Optional<String> admissionEndpoint(AwaitStepDescriptor descriptor) {
        return Optional.of("sqs://" + SqsConfig.from(descriptor.transportConfig()).requestQueueUrl());
    }

    @Override
    public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
        Objects.requireNonNull(request, "request must not be null");
        AwaitStepDescriptor descriptor = request.descriptor();
        AwaitInteractionRecord interaction = request.interaction();
        SqsConfig config = SqsConfig.from(descriptor.transportConfig());
        String resumeToken = resumeTokenService.sign(interaction, System.currentTimeMillis());
        Map<String, Object> metadata = dispatchMetadata(config);
        Object normalizedPayload = AwaitPayloadSupport.normalize(request.payload());
        SqsAwaitDispatchEnvelope envelope = SqsAwaitDispatchEnvelope.from(
            descriptor,
            interaction,
            normalizedPayload,
            resumeToken,
            metadata);
        return blocking(() -> serializeEnvelope(envelope))
            .onItem().transformToUni(body -> blocking(() -> {
                sqsClient().sendMessage(SendMessageRequest.builder()
                    .queueUrl(config.requestQueueUrl())
                    .messageBody(body)
                    .build());
                return new AwaitDispatchResult(metadata);
            }));
    }

    @PreDestroy
    void closeClient() {
        if (explicitClient != null) {
            return;
        }
        SqsClient active = client;
        if (active == null) {
            return;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } finally {
                client = null;
            }
        }
    }

    private SqsClient sqsClient() {
        SqsClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = SqsClient.builder();
                builder.httpClientBuilder(UrlConnectionHttpClient.builder());
                if (orchestratorConfig != null) {
                    orchestratorConfig.sqs().region()
                        .filter(region -> !region.isBlank())
                        .ifPresent(region -> builder.region(Region.of(region)));
                    orchestratorConfig.sqs().endpointOverride()
                        .filter(endpoint -> !endpoint.isBlank())
                        .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
                }
                client = builder.build();
            }
            return client;
        }
    }

    private static Map<String, Object> dispatchMetadata(SqsConfig config) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("adapter", "sqs");
        metadata.put("requestQueueUrl", config.requestQueueUrl());
        metadata.put("responseQueueUrl", config.responseQueueUrl());
        metadata.put("dispatchedAtEpochMs", System.currentTimeMillis());
        return metadata;
    }

    private static String serializeEnvelope(SqsAwaitDispatchEnvelope envelope) {
        try {
            return PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing SQS await dispatch envelope", e);
        }
    }

    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    record SqsConfig(String requestQueueUrl, String responseQueueUrl) {
        static SqsConfig from(Map<String, Object> transportConfig) {
            if (transportConfig == null) {
                throw new IllegalArgumentException("transportConfig must not be null");
            }
            Map<?, ?> request = requiredMap(transportConfig.get("request"), "sqs await transport requires request.queueUrl");
            Map<?, ?> response = requiredMap(transportConfig.get("response"), "sqs await transport requires response.queueUrl");
            return new SqsConfig(
                requiredString(request.get("queueUrl"), "sqs await transport requires request.queueUrl"),
                requiredString(response.get("queueUrl"), "sqs await transport requires response.queueUrl"));
        }

        private static Map<?, ?> requiredMap(Object value, String message) {
            if (value instanceof Map<?, ?> map) {
                return map;
            }
            throw new IllegalArgumentException(message);
        }

        private static String requiredString(Object value, String message) {
            if (value == null || value.toString().isBlank()) {
                throw new IllegalArgumentException(message);
            }
            return value.toString().trim();
        }

        public SqsConfig {
            if (normalizedQueueUrlForTypeCheck(requestQueueUrl).endsWith(".fifo")
                || normalizedQueueUrlForTypeCheck(responseQueueUrl).endsWith(".fifo")) {
                throw new IllegalArgumentException("sqs await transport supports standard queues only in v1");
            }
        }

        private static String normalizedQueueUrlForTypeCheck(String queueUrl) {
            String normalized = queueUrl.trim();
            int queryIndex = normalized.indexOf('?');
            if (queryIndex >= 0) {
                normalized = normalized.substring(0, queryIndex);
            }
            int fragmentIndex = normalized.indexOf('#');
            if (fragmentIndex >= 0) {
                normalized = normalized.substring(0, fragmentIndex);
            }
            return normalized;
        }
    }

    record SqsLiveAwaitWindowConfig(boolean pollerEnabled, Optional<String> responseQueueUrl) {
        static SqsLiveAwaitWindowConfig fromRuntime() {
            var config = ConfigProvider.getConfig();
            return new SqsLiveAwaitWindowConfig(
                config.getOptionalValue("tpf.await.sqs.poller.enabled", Boolean.class).orElse(false),
                config.getOptionalValue("tpf.await.sqs.response-queue-url", String.class)
                    .filter(value -> !value.isBlank())
                    .map(String::trim));
        }

        boolean matches(SqsConfig transportConfig) {
            return pollerEnabled
                && responseQueueUrl.map(transportConfig.responseQueueUrl()::equals).orElse(false);
        }
    }
}
