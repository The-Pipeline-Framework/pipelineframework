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
     * Selection of the orchestrator's execution mode.
     *
     * @return the configured OrchestratorMode
     */
    @WithDefault("SYNC")
    OrchestratorMode mode();

    /**
     * Default tenant ID used when callers do not provide one.
     *
     * @return the default tenant ID
     */
    @WithName("default-tenant")
    @WithDefault("default")
    String defaultTenant();

    /**
     * TTL for execution records, expressed in days.
     *
     * @return the number of days to retain execution records
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
     * Maximum number of asynchronous retry attempts before an execution is considered terminal.
     *
     * @return the maximum retry attempts before marking an execution as terminal
     */
    @WithName("max-retries")
    @WithDefault("3")
    int maxRetries();

    /**
     * Base delay applied between execution retry attempts.
     *
     * @return the base retry delay as a Duration (e.g., PT10S)
     */
    @WithName("retry-delay")
    @WithDefault("PT10S")
    Duration retryDelay();

    /**
     * Controls how the retry delay grows between successive attempts.
     *
     * @return Multiplier applied to the base retry delay for each retry attempt.
     */
    @WithName("retry-multiplier")
    @WithDefault("2.0")
    double retryMultiplier();

    /**
     * Interval between sweeper runs that redispatch due executions.
     *
     * @return the duration between sweeper executions
     */
    @WithName("sweep-interval")
    @WithDefault("PT30S")
    Duration sweepInterval();

    /**
     * Maximum number of due executions to redispatch in a single sweep.
     *
     * @return the maximum number of due executions processed in one sweep
     */
    @WithName("sweep-limit")
    @WithDefault("100")
    int sweepLimit();

    /**
     * Specifies the idempotency policy used for run-async submissions.
     *
     * @return the configured OrchestratorIdempotencyPolicy indicating whether an idempotency key is required, optional, or ignored for run-async requests
     */
    @WithName("idempotency-policy")
    @WithDefault("OPTIONAL_CLIENT_KEY")
    OrchestratorIdempotencyPolicy idempotencyPolicy();

    /**
     * Name of the state store provider to use for orchestrator state persistence.
     *
     * @return the configured state store provider name (default: "memory")
     */
    @WithName("state-provider")
    @WithDefault("memory")
    String stateProvider();

    /**
     * Name of the work dispatcher provider to use.
     *
     * @return the dispatcher provider name
     */
    @WithName("dispatcher-provider")
    @WithDefault("event")
    String dispatcherProvider();

    /**
     * The configured dead-letter publisher provider name.
     *
     * @return the dead-letter publisher (DLQ) provider name
     */
    @WithName("dlq-provider")
    @WithDefault("log")
    String dlqProvider();

    /**
     * Optional queue URL used by external dispatcher providers.
     *
     * @return an Optional containing the configured queue URL, or an empty Optional if not configured
     */
    @WithName("queue-url")
    Optional<String> queueUrl();

    /**
     * Controls whether application startup should fail when the orchestrator queue configuration is invalid.
     *
     * @return `true` if startup should fail when queue configuration is invalid, `false` otherwise.
     */
    @WithName("strict-startup")
    @WithDefault("true")
    boolean strictStartup();
}
