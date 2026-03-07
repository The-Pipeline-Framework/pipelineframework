package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Async orchestrator queue mode configuration.
 */
@ConfigMapping(prefix = "pipeline.orchestrator")
@Unremovable
public interface PipelineOrchestratorConfig {

    /**
     * Orchestrator execution mode.
     *
     * @return current mode
     */
    @WithDefault("SYNC")
    OrchestratorMode mode();

    /**
     * Default tenant when callers do not provide one.
     *
     * @return tenant id
     */
    @WithName("default-tenant")
    @WithDefault("default")
    String defaultTenant();

    /**
     * Execution record TTL in days.
     *
     * @return retention days
     */
    @WithName("execution-ttl-days")
    @WithDefault("7")
    int executionTtlDays();

    /**
     * Lease duration for one claimed execution.
     *
     * @return lease duration in milliseconds
     */
    @WithName("lease-ms")
    @WithDefault("30000")
    long leaseMs();

    /**
     * Maximum async retry attempts before terminal failure.
     *
     * @return retry attempts
     */
    @WithName("max-retries")
    @WithDefault("3")
    int maxRetries();

    /**
     * Base retry delay for execution-level retries.
     *
     * @return retry delay
     */
    @WithName("retry-delay")
    @WithDefault("PT10S")
    Duration retryDelay();

    /**
     * Delay multiplier applied for each attempt.
     *
     * @return retry delay multiplier
     */
    @WithName("retry-multiplier")
    @WithDefault("2.0")
    double retryMultiplier();

    /**
     * Sweeper interval that re-dispatches due executions.
     *
     * @return sweep interval
     */
    @WithName("sweep-interval")
    @WithDefault("PT30S")
    Duration sweepInterval();

    /**
     * Max due executions to sweep in one pass.
     *
     * @return sweep batch size
     */
    @WithName("sweep-limit")
    @WithDefault("100")
    int sweepLimit();

    /**
     * Idempotency key policy for run-async submissions.
     *
     * @return idempotency policy
     */
    @WithName("idempotency-policy")
    @WithDefault("OPTIONAL_CLIENT_KEY")
    OrchestratorIdempotencyPolicy idempotencyPolicy();

    /**
     * State store provider selection by provider name.
     *
     * @return provider name
     */
    @WithName("state-provider")
    @WithDefault("memory")
    String stateProvider();

    /**
     * Work dispatcher provider selection by provider name.
     *
     * @return provider name
     */
    @WithName("dispatcher-provider")
    @WithDefault("event")
    String dispatcherProvider();

    /**
     * Dead-letter publisher provider selection by provider name.
     *
     * @return provider name
     */
    @WithName("dlq-provider")
    @WithDefault("log")
    String dlqProvider();

    /**
     * Optional queue URL for external dispatcher providers.
     *
     * @return queue url when configured
     */
    @WithName("queue-url")
    Optional<String> queueUrl();

    /**
     * Optional dead-letter queue URL for durable DLQ providers.
     *
     * @return dead-letter queue url when configured
     */
    @WithName("dlq-url")
    Optional<String> dlqUrl();

    /**
     * DynamoDB provider configuration.
     *
     * @return dynamo provider config
     */
    @WithName("dynamo")
    DynamoConfig dynamo();

    /**
     * SQS provider configuration.
     *
     * @return sqs provider config
     */
    @WithName("sqs")
    SqsConfig sqs();

    /**
     * Enables strict startup guards in queue mode.
     *
     * @return true when startup should fail on invalid queue config
     */
    @WithName("strict-startup")
    @WithDefault("true")
    boolean strictStartup();

    /**
     * DynamoDB provider settings.
     */
    interface DynamoConfig {

        /**
         * Execution table name.
         *
         * @return execution table name
         */
        @WithName("execution-table")
        @WithDefault("tpf_execution")
        String executionTable();

        /**
         * Execution key deduplication table name.
         *
         * @return execution key table name
         */
        @WithName("execution-key-table")
        @WithDefault("tpf_execution_key")
        String executionKeyTable();

        /**
         * Optional region override.
         *
         * @return region when configured
         */
        @WithName("region")
        Optional<String> region();

        /**
         * Optional endpoint override, typically for local development.
         *
         * @return endpoint URI when configured
         */
        @WithName("endpoint-override")
        Optional<String> endpointOverride();
    }

    /**
     * SQS provider settings.
     */
    interface SqsConfig {

        /**
         * Optional region override.
         *
         * @return region when configured
         */
        @WithName("region")
        Optional<String> region();

        /**
         * Optional endpoint override, typically for local development.
         *
         * @return endpoint URI when configured
         */
        @WithName("endpoint-override")
        Optional<String> endpointOverride();

        /**
         * Enables local in-process dispatch in addition to SQS enqueue.
         *
         * @return true to dispatch locally after enqueueing in SQS
         */
        @WithName("local-loopback")
        @WithDefault("true")
        boolean localLoopback();
    }
}
