package org.pipelineframework.orchestrator;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineOrchestratorConfigTest {

    @Test
    void configUsesDefaultValues() {
        PipelineOrchestratorConfig config = buildConfig(Map.of());

        assertEquals(OrchestratorMode.SYNC, config.mode());
        assertEquals("default", config.defaultTenant());
        assertEquals(7, config.executionTtlDays());
        assertEquals(30000L, config.leaseMs());
        assertEquals(3, config.maxRetries());
        assertEquals(Duration.ofSeconds(10), config.retryDelay());
        assertEquals(2.0, config.retryMultiplier());
        assertEquals(Duration.ofSeconds(30), config.sweepInterval());
        assertEquals(100, config.sweepLimit());
        assertEquals(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY, config.idempotencyPolicy());
        assertEquals("memory", config.stateProvider());
        assertEquals("event", config.dispatcherProvider());
        assertEquals("log", config.dlqProvider());
        assertTrue(config.strictStartup());
        assertFalse(config.queueUrl().isPresent());
        assertFalse(config.dlqUrl().isPresent());
        assertFalse(config.resumeTokenSecret().isPresent());
        assertEquals(10000, config.awaitAggregateMaxInputItems());
        assertEquals(10000, config.awaitAggregateMaxOutputItems());
        assertEquals(List.of("org.pipelineframework."), config.worker().allowedPayloadPrefixes());
        assertFalse(config.workerRest().sharedSecret().isPresent());
        assertFalse(config.workerRest().sharedSecretRef().isPresent());
        assertEquals(Duration.ofMinutes(2), config.workerRest().signatureTolerance());
        assertFalse(config.workerGrpc().endpoint().isPresent());
        assertFalse(config.workerGrpc().plaintext());
        assertEquals(Duration.ofSeconds(30), config.workerGrpc().requestTimeout());
        assertFalse(config.workerGrpc().serverEnabled());
        assertFalse(config.workerGrpc().sharedSecret().isPresent());
        assertFalse(config.workerGrpc().sharedSecretRef().isPresent());
        assertEquals(Duration.ofMinutes(2), config.workerGrpc().signatureTolerance());
        assertFalse(config.workerSqs().requestQueueUrl().isPresent());
        assertFalse(config.workerSqs().responseQueueUrl().isPresent());
        assertFalse(config.workerSqs().serverEnabled());
        assertEquals(Duration.ofSeconds(30), config.workerSqs().requestTimeout());
        assertEquals(Duration.ZERO, config.workerSqs().pollStartDelay());
        assertEquals(Duration.ofSeconds(30), config.workerSqs().visibilityTimeout());
        assertFalse(config.workerSqs().sharedSecret().isPresent());
        assertFalse(config.workerSqs().sharedSecretRef().isPresent());
        assertEquals(Duration.ofMinutes(2), config.workerSqs().signatureTolerance());
    }

    @ParameterizedTest(name = "{index} => {0}={1}")
    @MethodSource("singlePropertyOverrides")
    void configReadsSinglePropertyOverride(
        String propertyKey,
        String propertyValue,
        Consumer<PipelineOrchestratorConfig> assertion
    ) {
        PipelineOrchestratorConfig config = buildConfig(Map.of(propertyKey, propertyValue));
        assertion.accept(config);
    }

    @Test
    void dynamoConfigUsesDefaultValues() {
        PipelineOrchestratorConfig config = buildConfig(Map.of());
        PipelineOrchestratorConfig.DynamoConfig dynamoConfig = config.dynamo();

        assertNotNull(dynamoConfig);
        assertEquals("tpf_execution", dynamoConfig.executionTable());
        assertEquals("tpf_execution_key", dynamoConfig.executionKeyTable());
        assertEquals("tpf_execution_payload", dynamoConfig.executionPayloadTable());
        assertEquals("tpf_release_registry", dynamoConfig.releaseTable());
        assertFalse(dynamoConfig.region().isPresent());
        assertFalse(dynamoConfig.endpointOverride().isPresent());
    }


    @Test
    void sqsConfigUsesDefaultValues() {
        PipelineOrchestratorConfig config = buildConfig(Map.of());
        PipelineOrchestratorConfig.SqsConfig sqsConfig = config.sqs();

        assertNotNull(sqsConfig);
        assertFalse(sqsConfig.region().isPresent());
        assertFalse(sqsConfig.endpointOverride().isPresent());
        assertTrue(sqsConfig.localLoopback());
    }

    private static Stream<Arguments> singlePropertyOverrides() {
        return Stream.of(
            Arguments.of(
                "pipeline.orchestrator.mode",
                "QUEUE_ASYNC",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(OrchestratorMode.QUEUE_ASYNC, config.mode())),
            Arguments.of(
                "pipeline.orchestrator.default-tenant",
                "custom-tenant",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("custom-tenant", config.defaultTenant())),
            Arguments.of(
                "pipeline.orchestrator.execution-ttl-days",
                "14",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(14, config.executionTtlDays())),
            Arguments.of(
                "pipeline.orchestrator.lease-ms",
                "60000",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(60000L, config.leaseMs())),
            Arguments.of(
                "pipeline.orchestrator.max-retries",
                "5",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(5, config.maxRetries())),
            Arguments.of(
                "pipeline.orchestrator.retry-delay",
                "PT15S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(15), config.retryDelay())),
            Arguments.of(
                "pipeline.orchestrator.retry-multiplier",
                "3.0",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(3.0, config.retryMultiplier())),
            Arguments.of(
                "pipeline.orchestrator.sweep-interval",
                "PT1M",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofMinutes(1), config.sweepInterval())),
            Arguments.of(
                "pipeline.orchestrator.sweep-limit",
                "200",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(200, config.sweepLimit())),
            Arguments.of(
                "pipeline.orchestrator.idempotency-policy",
                "CLIENT_KEY_REQUIRED",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED, config.idempotencyPolicy())),
            Arguments.of(
                "pipeline.orchestrator.state-provider",
                "dynamo",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("dynamo", config.stateProvider())),
            Arguments.of(
                "pipeline.orchestrator.dispatcher-provider",
                "sqs",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("sqs", config.dispatcherProvider())),
            Arguments.of(
                "pipeline.orchestrator.dlq-provider",
                "sqs",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("sqs", config.dlqProvider())),
            Arguments.of(
                "pipeline.orchestrator.queue-url",
                "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
                (Consumer<PipelineOrchestratorConfig>) config -> {
                    Optional<String> queueUrl = config.queueUrl();
                    assertTrue(queueUrl.isPresent());
                    assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", queueUrl.get());
                }),
            Arguments.of(
                "pipeline.orchestrator.dlq-url",
                "https://sqs.us-east-1.amazonaws.com/123456789012/my-dlq",
                (Consumer<PipelineOrchestratorConfig>) config -> {
                    Optional<String> dlqUrl = config.dlqUrl();
                    assertTrue(dlqUrl.isPresent());
                    assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/my-dlq", dlqUrl.get());
                }),
            Arguments.of(
                "pipeline.orchestrator.resume-token-secret",
                "test-secret",
                (Consumer<PipelineOrchestratorConfig>) config -> {
                    Optional<String> secret = config.resumeTokenSecret();
                    assertTrue(secret.isPresent());
                    assertEquals("test-secret", secret.get());
                }),
            Arguments.of(
                "pipeline.orchestrator.await-aggregate-max-input-items",
                "500",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(500, config.awaitAggregateMaxInputItems())),
            Arguments.of(
                "pipeline.orchestrator.await-aggregate-max-output-items",
                "250",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(250, config.awaitAggregateMaxOutputItems())),
            Arguments.of(
                "pipeline.orchestrator.control-plane.enabled",
                "true",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertTrue(config.controlPlane().enabled())),
            Arguments.of(
                "pipeline.orchestrator.control-plane.require-remote-worker",
                "true",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertTrue(config.controlPlane().requireRemoteWorker())),
            Arguments.of(
                "pipeline.orchestrator.control-plane.admin-token",
                "admin-secret",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("admin-secret", config.controlPlane().adminToken().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.control-plane.admin-token-ref",
                "env:TPF_CONTROL_PLANE_ADMIN_TOKEN",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("env:TPF_CONTROL_PLANE_ADMIN_TOKEN", config.controlPlane().adminTokenRef().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.allowed-payload-prefixes",
                "com.example.,org.acme.",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(List.of("com.example.", "org.acme."), config.worker().allowedPayloadPrefixes())),
            Arguments.of(
                "pipeline.orchestrator.worker.rest.shared-secret",
                "worker-secret",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("worker-secret", config.workerRest().sharedSecret().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.rest.shared-secret-ref",
                "env:TPF_REST_WORKER_SECRET",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("env:TPF_REST_WORKER_SECRET", config.workerRest().sharedSecretRef().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.rest.signature-tolerance",
                "PT30S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(30), config.workerRest().signatureTolerance())),
            Arguments.of(
                "pipeline.orchestrator.worker.grpc.endpoint",
                "localhost:9000",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("localhost:9000", config.workerGrpc().endpoint().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.grpc.plaintext",
                "true",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertTrue(config.workerGrpc().plaintext())),
            Arguments.of(
                "pipeline.orchestrator.worker.grpc.request-timeout",
                "PT5S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(5), config.workerGrpc().requestTimeout())),
            Arguments.of(
                "pipeline.orchestrator.worker.grpc.server-enabled",
                "true",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertTrue(config.workerGrpc().serverEnabled())),
            Arguments.of(
                "pipeline.orchestrator.worker.grpc.shared-secret",
                "grpc-worker-secret",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("grpc-worker-secret", config.workerGrpc().sharedSecret().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.grpc.shared-secret-ref",
                "sys:tpf.grpc.worker.secret",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("sys:tpf.grpc.worker.secret", config.workerGrpc().sharedSecretRef().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.grpc.signature-tolerance",
                "PT45S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(45), config.workerGrpc().signatureTolerance())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.request-queue-url",
                "https://sqs.local/request",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("https://sqs.local/request", config.workerSqs().requestQueueUrl().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.response-queue-url",
                "https://sqs.local/response",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("https://sqs.local/response", config.workerSqs().responseQueueUrl().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.server-enabled",
                "true",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertTrue(config.workerSqs().serverEnabled())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.request-timeout",
                "PT8S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(8), config.workerSqs().requestTimeout())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.poll-start-delay",
                "PT3S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(3), config.workerSqs().pollStartDelay())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.visibility-timeout",
                "PT45S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(45), config.workerSqs().visibilityTimeout())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.shared-secret",
                "sqs-worker-secret",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("sqs-worker-secret", config.workerSqs().sharedSecret().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.shared-secret-ref",
                "config:tpf.sqs.worker.secret",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("config:tpf.sqs.worker.secret", config.workerSqs().sharedSecretRef().orElseThrow())),
            Arguments.of(
                "pipeline.orchestrator.worker.sqs.signature-tolerance",
                "PT50S",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals(Duration.ofSeconds(50), config.workerSqs().signatureTolerance())),
            Arguments.of(
                "pipeline.orchestrator.strict-startup",
                "false",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertFalse(config.strictStartup())),
            Arguments.of(
                "pipeline.orchestrator.dynamo.execution-table",
                "custom_execution",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("custom_execution", config.dynamo().executionTable())),
            Arguments.of(
                "pipeline.orchestrator.dynamo.execution-key-table",
                "custom_execution_key",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("custom_execution_key", config.dynamo().executionKeyTable())),
            Arguments.of(
                "pipeline.orchestrator.dynamo.execution-payload-table",
                "custom_execution_payload",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("custom_execution_payload", config.dynamo().executionPayloadTable())),
            Arguments.of(
                "pipeline.orchestrator.dynamo.release-table",
                "custom_release_registry",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertEquals("custom_release_registry", config.dynamo().releaseTable())),
            Arguments.of(
                "pipeline.orchestrator.dynamo.region",
                "us-west-2",
                (Consumer<PipelineOrchestratorConfig>) config -> {
                    Optional<String> region = config.dynamo().region();
                    assertTrue(region.isPresent());
                    assertEquals("us-west-2", region.get());
                }),
            Arguments.of(
                "pipeline.orchestrator.dynamo.endpoint-override",
                "http://localhost:8000",
                (Consumer<PipelineOrchestratorConfig>) config -> {
                    Optional<String> endpoint = config.dynamo().endpointOverride();
                    assertTrue(endpoint.isPresent());
                    assertEquals("http://localhost:8000", endpoint.get());
                }),
            Arguments.of(
                "pipeline.orchestrator.sqs.region",
                "eu-west-1",
                (Consumer<PipelineOrchestratorConfig>) config -> {
                    Optional<String> region = config.sqs().region();
                    assertTrue(region.isPresent());
                    assertEquals("eu-west-1", region.get());
                }),
            Arguments.of(
                "pipeline.orchestrator.sqs.endpoint-override",
                "http://localhost:9324",
                (Consumer<PipelineOrchestratorConfig>) config -> {
                    Optional<String> endpoint = config.sqs().endpointOverride();
                    assertTrue(endpoint.isPresent());
                    assertEquals("http://localhost:9324", endpoint.get());
                }),
            Arguments.of(
                "pipeline.orchestrator.sqs.local-loopback",
                "false",
                (Consumer<PipelineOrchestratorConfig>) config ->
                    assertFalse(config.sqs().localLoopback()))
        );
    }

    @Test
    void configSupportsQueueAsyncProduction() {
        Map<String, String> props = new HashMap<>();
        props.put("pipeline.orchestrator.mode", "QUEUE_ASYNC");
        props.put("pipeline.orchestrator.state-provider", "dynamo");
        props.put("pipeline.orchestrator.dispatcher-provider", "sqs");
        props.put("pipeline.orchestrator.queue-url", "https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-work");
        props.put("pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED");
        props.put("pipeline.orchestrator.dynamo.execution-table", "prod_execution");
        props.put("pipeline.orchestrator.dynamo.execution-key-table", "prod_execution_key");
        props.put("pipeline.orchestrator.dynamo.execution-payload-table", "prod_execution_payload");
        props.put("pipeline.orchestrator.dynamo.region", "eu-west-1");
        props.put("pipeline.orchestrator.sqs.region", "eu-west-1");
        props.put("pipeline.orchestrator.sqs.local-loopback", "false");
        props.put("pipeline.orchestrator.strict-startup", "true");

        PipelineOrchestratorConfig config = buildConfig(props);

        assertEquals(OrchestratorMode.QUEUE_ASYNC, config.mode());
        assertEquals("dynamo", config.stateProvider());
        assertEquals("sqs", config.dispatcherProvider());
        assertEquals("https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-work", config.queueUrl().get());
        assertEquals(OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED, config.idempotencyPolicy());
        assertEquals("prod_execution", config.dynamo().executionTable());
        assertEquals("prod_execution_key", config.dynamo().executionKeyTable());
        assertEquals("prod_execution_payload", config.dynamo().executionPayloadTable());
        assertEquals("eu-west-1", config.dynamo().region().get());
        assertEquals("eu-west-1", config.sqs().region().get());
        assertFalse(config.sqs().localLoopback());
        assertTrue(config.strictStartup());
    }

    @Test
    void configSupportsLocalDevelopmentMode() {
        Map<String, String> props = new HashMap<>();
        props.put("pipeline.orchestrator.mode", "QUEUE_ASYNC");
        props.put("pipeline.orchestrator.state-provider", "memory");
        props.put("pipeline.orchestrator.dispatcher-provider", "event");
        props.put("pipeline.orchestrator.idempotency-policy", "OPTIONAL_CLIENT_KEY");
        props.put("pipeline.orchestrator.strict-startup", "false");

        PipelineOrchestratorConfig config = buildConfig(props);

        assertEquals(OrchestratorMode.QUEUE_ASYNC, config.mode());
        assertEquals("memory", config.stateProvider());
        assertEquals("event", config.dispatcherProvider());
        assertFalse(config.queueUrl().isPresent());
        assertEquals(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY, config.idempotencyPolicy());
        assertFalse(config.strictStartup());
    }

    @Test
    void configSupportsDynamoWithLocalEndpoint() {
        Map<String, String> props = new HashMap<>();
        props.put("pipeline.orchestrator.mode", "QUEUE_ASYNC");
        props.put("pipeline.orchestrator.state-provider", "dynamo");
        props.put("pipeline.orchestrator.dynamo.endpoint-override", "http://localhost:8000");

        PipelineOrchestratorConfig config = buildConfig(props);

        assertEquals("http://localhost:8000", config.dynamo().endpointOverride().get());
    }

    @Test
    void configSupportsSqsWithLocalEndpoint() {
        Map<String, String> props = new HashMap<>();
        props.put("pipeline.orchestrator.mode", "QUEUE_ASYNC");
        props.put("pipeline.orchestrator.dispatcher-provider", "sqs");
        props.put("pipeline.orchestrator.sqs.endpoint-override", "http://localhost:9324");

        PipelineOrchestratorConfig config = buildConfig(props);

        assertEquals("http://localhost:9324", config.sqs().endpointOverride().get());
    }

    @Test
    void retryConfigurationCanBeCustomized() {
        Map<String, String> props = new HashMap<>();
        props.put("pipeline.orchestrator.max-retries", "10");
        props.put("pipeline.orchestrator.retry-delay", "PT5S");
        props.put("pipeline.orchestrator.retry-multiplier", "1.5");

        PipelineOrchestratorConfig config = buildConfig(props);

        assertEquals(10, config.maxRetries());
        assertEquals(Duration.ofSeconds(5), config.retryDelay());
        assertEquals(1.5, config.retryMultiplier());
    }

    @Test
    void sweepConfigurationCanBeCustomized() {
        Map<String, String> props = new HashMap<>();
        props.put("pipeline.orchestrator.sweep-interval", "PT1M");
        props.put("pipeline.orchestrator.sweep-limit", "500");

        PipelineOrchestratorConfig config = buildConfig(props);

        assertEquals(Duration.ofMinutes(1), config.sweepInterval());
        assertEquals(500, config.sweepLimit());
    }

    private PipelineOrchestratorConfig buildConfig(Map<String, String> properties) {
        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder();
        builder.withMapping(PipelineOrchestratorConfig.class);
        builder.withSources(new ConfigSource() {
            @Override
            public Map<String, String> getProperties() {
                return properties;
            }

            @Override
            public Set<String> getPropertyNames() {
                return properties.keySet();
            }

            @Override
            public String getValue(String propertyName) {
                return properties.get(propertyName);
            }

            @Override
            public String getName() {
                return "test-config";
            }
        });

        SmallRyeConfig smallRyeConfig = builder.build();
        return smallRyeConfig.getConfigMapping(PipelineOrchestratorConfig.class);
    }
}
