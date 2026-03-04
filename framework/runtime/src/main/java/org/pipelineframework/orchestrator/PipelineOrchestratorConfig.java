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
     * Determines the orchestrator execution mode.
     *
     * @return the configured orchestrator execution mode
     */
    @WithDefault("SYNC")
    OrchestratorMode mode();

    /**
     * Default tenant identifier used when no tenant is provided by the caller.
     *
     * @return the default tenant identifier
     */
    @WithName("default-tenant")
    @WithDefault("default")
    String defaultTenant();

    /**
     * Number of days to retain execution records.
     *
     * @return the retention period in days
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
     * Maximum number of retry attempts for asynchronous executions before the execution is considered a terminal failure.
     *
     * @return the maximum retry attempts
     */
    @WithName("max-retries")
    @WithDefault("3")
    int maxRetries();

    /**
     * Base delay used between execution retries.
     *
     * @return the delay Duration applied before each execution retry
     */
    @WithName("retry-delay")
    @WithDefault("PT10S")
    Duration retryDelay();

    /**
     * Multiplier applied to the base retry delay for successive retry attempts.
     *
     * @return the factor by which the previous retry delay is multiplied for the next attempt
     */
    @WithName("retry-multiplier")
    @WithDefault("2.0")
    double retryMultiplier();

    /**
     * Interval between sweeper runs that re-dispatch due executions.
     *
     * @return the duration between sweeper runs
     */
    @WithName("sweep-interval")
    @WithDefault("PT30S")
    Duration sweepInterval();

    /**
     * Maximum number of due executions to sweep in a single pass.
     *
     * @return the maximum number of due executions processed in one sweep
     */
    @WithName("sweep-limit")
    @WithDefault("100")
    int sweepLimit();

    /**
     * Determines the idempotency key policy used for run-async submissions.
     *
     * @return the idempotency policy applied to run-async submissions
     */
    @WithName("idempotency-policy")
    @WithDefault("OPTIONAL_CLIENT_KEY")
    OrchestratorIdempotencyPolicy idempotencyPolicy();

    /**
     * Selects the state store provider by name for the orchestrator.
     *
     * @return the configured state store provider name (defaults to "memory")
     */
    @WithName("state-provider")
    @WithDefault("memory")
    String stateProvider();

    /**
     * Selects which dispatcher provider to use by name.
     *
     * @return the configured dispatcher provider name
     */
    @WithName("dispatcher-provider")
    @WithDefault("event")
    String dispatcherProvider();

    /**
     * Specifies the dead-letter queue publisher provider to use.
     *
     * @return the configured provider name
     */
    @WithName("dlq-provider")
    @WithDefault("log")
    String dlqProvider();

    /**
     * Queue URL used by external dispatcher providers.
     *
     * @return the configured queue URL, or empty if not set
     */
    @WithName("queue-url")
    Optional<String> queueUrl();

    /**
     * Enable strict startup guards for queue mode.
     *
     * @return `true` if startup should fail when the queue configuration is invalid, `false` otherwise.
     */
    @WithName("strict-startup")
    @WithDefault("true")
    boolean strictStartup();
}
