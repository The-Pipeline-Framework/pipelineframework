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
     * Enables strict startup guards in queue mode.
     *
     * @return true when startup should fail on invalid queue config
     */
    @WithName("strict-startup")
    @WithDefault("true")
    boolean strictStartup();
}
