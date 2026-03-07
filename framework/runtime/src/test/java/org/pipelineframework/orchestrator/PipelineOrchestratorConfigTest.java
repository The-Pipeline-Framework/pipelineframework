package org.pipelineframework.orchestrator;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
    }

    @Test
    void configReadsOrchestratorModeFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.mode", "QUEUE_ASYNC"
        ));

        assertEquals(OrchestratorMode.QUEUE_ASYNC, config.mode());
    }

    @Test
    void configReadsDefaultTenantFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.default-tenant", "custom-tenant"
        ));

        assertEquals("custom-tenant", config.defaultTenant());
    }

    @Test
    void configReadsExecutionTtlDaysFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.execution-ttl-days", "14"
        ));

        assertEquals(14, config.executionTtlDays());
    }

    @Test
    void configReadsLeaseMsFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.lease-ms", "60000"
        ));

        assertEquals(60000L, config.leaseMs());
    }

    @Test
    void configReadsMaxRetriesFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.max-retries", "5"
        ));

        assertEquals(5, config.maxRetries());
    }

    @Test
    void configReadsRetryDelayFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.retry-delay", "PT15S"
        ));

        assertEquals(Duration.ofSeconds(15), config.retryDelay());
    }

    @Test
    void configReadsRetryMultiplierFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.retry-multiplier", "3.0"
        ));

        assertEquals(3.0, config.retryMultiplier());
    }

    @Test
    void configReadsSweepIntervalFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.sweep-interval", "PT1M"
        ));

        assertEquals(Duration.ofMinutes(1), config.sweepInterval());
    }

    @Test
    void configReadsSweepLimitFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.sweep-limit", "200"
        ));

        assertEquals(200, config.sweepLimit());
    }

    @Test
    void configReadsIdempotencyPolicyFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED"
        ));

        assertEquals(OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED, config.idempotencyPolicy());
    }

    @Test
    void configReadsStateProviderFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.state-provider", "dynamo"
        ));

        assertEquals("dynamo", config.stateProvider());
    }

    @Test
    void configReadsDispatcherProviderFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.dispatcher-provider", "sqs"
        ));

        assertEquals("sqs", config.dispatcherProvider());
    }

    @Test
    void configReadsDlqProviderFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.dlq-provider", "sqs"
        ));

        assertEquals("sqs", config.dlqProvider());
    }

    @Test
    void configReadsQueueUrlFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.queue-url", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
        ));

        Optional<String> queueUrl = config.queueUrl();
        assertTrue(queueUrl.isPresent());
        assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", queueUrl.get());
    }

    @Test
    void configReadsStrictStartupFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.strict-startup", "false"
        ));

        assertFalse(config.strictStartup());
    }

    @Test
    void dynamoConfigUsesDefaultValues() {
        PipelineOrchestratorConfig config = buildConfig(Map.of());
        PipelineOrchestratorConfig.DynamoConfig dynamoConfig = config.dynamo();

        assertNotNull(dynamoConfig);
        assertEquals("tpf_execution", dynamoConfig.executionTable());
        assertEquals("tpf_execution_key", dynamoConfig.executionKeyTable());
        assertFalse(dynamoConfig.region().isPresent());
        assertFalse(dynamoConfig.endpointOverride().isPresent());
    }

    @Test
    void dynamoConfigReadsExecutionTableFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.dynamo.execution-table", "custom_execution"
        ));

        assertEquals("custom_execution", config.dynamo().executionTable());
    }

    @Test
    void dynamoConfigReadsExecutionKeyTableFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.dynamo.execution-key-table", "custom_execution_key"
        ));

        assertEquals("custom_execution_key", config.dynamo().executionKeyTable());
    }

    @Test
    void dynamoConfigReadsRegionFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.dynamo.region", "us-west-2"
        ));

        Optional<String> region = config.dynamo().region();
        assertTrue(region.isPresent());
        assertEquals("us-west-2", region.get());
    }

    @Test
    void dynamoConfigReadsEndpointOverrideFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.dynamo.endpoint-override", "http://localhost:8000"
        ));

        Optional<String> endpoint = config.dynamo().endpointOverride();
        assertTrue(endpoint.isPresent());
        assertEquals("http://localhost:8000", endpoint.get());
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

    @Test
    void sqsConfigReadsRegionFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.sqs.region", "eu-west-1"
        ));

        Optional<String> region = config.sqs().region();
        assertTrue(region.isPresent());
        assertEquals("eu-west-1", region.get());
    }

    @Test
    void sqsConfigReadsEndpointOverrideFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.sqs.endpoint-override", "http://localhost:9324"
        ));

        Optional<String> endpoint = config.sqs().endpointOverride();
        assertTrue(endpoint.isPresent());
        assertEquals("http://localhost:9324", endpoint.get());
    }

    @Test
    void sqsConfigReadsLocalLoopbackFromProperties() {
        PipelineOrchestratorConfig config = buildConfig(Map.of(
            "pipeline.orchestrator.sqs.local-loopback", "false"
        ));

        assertFalse(config.sqs().localLoopback());
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