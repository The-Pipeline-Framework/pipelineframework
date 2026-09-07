package org.pipelineframework.config;

import java.time.Duration;
import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

/**
 * Opt-in runtime configuration for transport-boundary resilience policies.
 */
@ConfigMapping(prefix = "pipeline.resilience")
@Unremovable
public interface PipelineResilienceConfig {

    /**
     * Deployment-provided circuit guarantee used unless a boundary requests a compatible override.
     *
     * @return default circuit scope for this runtime
     */
    @WithName("default-circuit-scope")
    @WithDefault("LOCAL_PROCESS")
    CircuitScope defaultCircuitScope();

    /** Shared-circuit authority configuration. Required only by SHARED_DEPENDENCY policies. */
    SharedConfig shared();

    interface SharedConfig {
        /** DynamoDB table forming the single-region shared protection domain. */
        @WithName("dynamo-table")
        Optional<String> dynamoTable();

        /** Maximum age of a locally cached shared state snapshot. */
        @WithName("max-state-staleness")
        @WithDefault("PT1S")
        Duration maxStateStaleness();

        /** Hint returned after a shared authority outage. */
        @WithName("backend-retry-delay")
        @WithDefault("PT1S")
        Duration backendRetryDelay();

        /** Optional AWS region; the default provider chain is used when omitted. */
        Optional<String> region();

        /** Optional Dynamo endpoint override, primarily for isolated environments. */
        @WithName("endpoint-override")
        Optional<String> endpointOverride();
    }
}
