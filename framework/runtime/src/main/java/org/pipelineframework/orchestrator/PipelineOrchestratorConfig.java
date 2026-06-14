package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.List;
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
     * Logical pipeline identifier used in transition-worker envelopes.
     *
     * @return pipeline id
     */
    @WithName("pipeline-id")
    @WithDefault("local-pipeline")
    String pipelineId();

    /**
     * Release version identifier used in transition-worker envelopes.
     *
     * @return release version
     */
    @WithName("release-version")
    @WithDefault("local-contract")
    String releaseVersion();

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
     * Secret used to sign and verify await resume tokens.
     *
     * @return configured token secret
     */
    @WithName("resume-token-secret")
    Optional<String> resumeTokenSecret();

    /**
     * Maximum number of items an aggregate await step may materialize from its input stream.
     *
     * A value of zero disables the guard.
     *
     * @return max materialized input items for aggregate await units
     */
    @WithName("await-aggregate-max-input-items")
    @WithDefault("10000")
    int awaitAggregateMaxInputItems();

    /**
     * Maximum number of items an aggregate await step may replay from one materialized output unit.
     *
     * A value of zero disables the guard.
     *
     * @return max materialized output items for aggregate await units
     */
    @WithName("await-aggregate-max-output-items")
    @WithDefault("10000")
    int awaitAggregateMaxOutputItems();

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
     * Local tenancy admission settings.
     *
     * @return tenancy config
     */
    @WithName("tenancy")
    TenancyConfig tenancy();

    /**
     * Local quota admission settings.
     *
     * @return quota config
     */
    @WithName("quotas")
    QuotaConfig quotas();

    /**
     * Hosted control-plane API settings.
     *
     * @return control-plane API config
     */
    @WithName("control-plane")
    ControlPlaneConfig controlPlane();

    /**
     * Local/dev administration API settings.
     *
     * @return admin API config
     */
    @WithName("admin")
    AdminConfig admin();

    /**
     * Hosted release registry and artifact storage settings.
     *
     * @return release config
     */
    @WithName("releases")
    ReleasesConfig releases();

    /**
     * Transition worker execution settings.
     *
     * @return worker config
     */
    @WithName("worker")
    WorkerConfig worker();

    /**
     * REST transition worker settings.
     *
     * @return REST worker config
     */
    default RestWorkerConfig workerRest() {
        return worker().rest();
    }

    /**
     * gRPC transition worker settings.
     *
     * @return gRPC worker config
     */
    default GrpcWorkerConfig workerGrpc() {
        return worker().grpc();
    }

    /**
     * SQS transition worker settings.
     *
     * @return SQS worker config
     */
    default SqsWorkerConfig workerSqs() {
        return worker().sqs();
    }

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
         * Await interaction table name.
         *
         * @return await interaction table name
         */
        @WithName("await-interaction-table")
        @WithDefault("tpf_await_interaction")
        String awaitInteractionTable();

        /**
         * Await interaction lookup table name.
         *
         * @return await lookup table name
         */
        @WithName("await-interaction-key-table")
        @WithDefault("tpf_await_interaction_key")
        String awaitInteractionKeyTable();

        /**
         * Await unit table name.
         *
         * @return await unit table name
         */
        @WithName("await-unit-table")
        @WithDefault("tpf_await_unit")
        String awaitUnitTable();

        /**
         * Release registry metadata table name.
         *
         * @return release registry table name
         */
        @WithName("release-table")
        @WithDefault("tpf_release_registry")
        String releaseTable();

        /**
         * Worker lifecycle metadata table name.
         *
         * @return worker lifecycle table name
         */
        @WithName("worker-table")
        @WithDefault("tpf_worker_registry")
        String workerTable();

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
         * Optional delay before the SQS poller starts consuming work.
         *
         * @return poller start delay
         */
        @WithName("poll-start-delay")
        @WithDefault("PT0S")
        Duration pollStartDelay();

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

    /**
     * Local control-plane tenancy settings.
     */
    interface TenancyConfig {

        /**
         * Allowed tenant ids. Empty means all tenants are allowed.
         *
         * @return allowed tenant ids
         */
        default List<String> allowedTenants() {
            return allowedTenantsConfig().orElse(List.of());
        }

        /**
         * Raw optional allowed tenant ids from runtime config.
         *
         * @return configured tenant ids when present
         */
        @WithName("allowed-tenants")
        Optional<List<String>> allowedTenantsConfig();

        /**
         * Requires callers to provide tenant id for async submission instead of using default tenant.
         *
         * @return true when tenant id must be explicit for submissions
         */
        @WithName("require-explicit-tenant")
        @WithDefault("false")
        boolean requireExplicitTenant();
    }

    /**
     * Local control-plane quota settings.
     */
    interface QuotaConfig {

        /**
         * Maximum active transitions per tenant in this runtime instance. Zero disables the limit.
         *
         * @return max in-flight transitions per tenant
         */
        @WithName("max-in-flight-transitions-per-tenant")
        @WithDefault("0")
        int maxInFlightTransitionsPerTenant();
    }

    /**
     * Local/dev hosted control-plane API settings.
     */
    interface ControlPlaneConfig {

        /**
         * Enables the internal hosted control-plane REST surface.
         *
         * @return true when the resource should accept requests
         */
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        /**
         * Requires the hosted control-plane runtime to dispatch to a configured remote transition worker.
         *
         * @return true when local in-process worker fallback should fail startup
         */
        @WithName("require-remote-worker")
        @WithDefault("false")
        boolean requireRemoteWorker();

        /**
         * Literal bearer token for local/dev coordinator API calls.
         *
         * @return admin token when configured
         */
        @WithName("admin-token")
        Optional<String> adminToken();

        /**
         * Secret reference for the local/dev coordinator API bearer token.
         *
         * @return admin token reference when configured
         */
        @WithName("admin-token-ref")
        Optional<String> adminTokenRef();
    }

    /**
     * Local/dev administration API settings.
     */
    interface AdminConfig {

        /**
         * Enables the internal admin REST surface.
         *
         * @return true when the resource should accept requests
         */
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        /**
         * Literal bearer token for local/dev admin calls.
         *
         * @return admin token when configured
         */
        @WithName("admin-token")
        Optional<String> adminToken();

        /**
         * Secret reference for the local/dev admin bearer token.
         *
         * @return admin token reference when configured
         */
        @WithName("admin-token-ref")
        Optional<String> adminTokenRef();
    }

    /**
     * Release registry and artifact storage settings.
     */
    interface ReleasesConfig {

        /**
         * Release registry metadata provider settings.
         *
         * @return registry config
         */
        @WithName("registry")
        ReleaseRegistryConfig registry();

        /**
         * Release artifact storage settings.
         *
         * @return storage config
         */
        @WithName("storage")
        ReleaseStorageConfig storage();
    }

    /**
     * Release registry provider settings.
     */
    interface ReleaseRegistryConfig {

        /**
         * Registry provider name.
         *
         * @return memory, file, or dynamo
         */
        @WithName("provider")
        @WithDefault("memory")
        String provider();
    }

    /**
     * Release artifact storage settings.
     */
    interface ReleaseStorageConfig {

        /**
         * Release artifact storage provider name.
         *
         * @return local or s3
         */
        @WithName("provider")
        @WithDefault("local")
        String provider();

        /**
         * Local root directory for managed release artifacts and file-backed registry metadata.
         *
         * @return storage root path
         */
        @WithName("root")
        @WithDefault("target/tpf-releases")
        String root();

        /**
         * S3-compatible release artifact storage settings.
         *
         * @return S3 storage config
         */
        @WithName("s3")
        ReleaseStorageS3Config s3();
    }

    /**
     * S3-compatible release artifact storage settings.
     */
    interface ReleaseStorageS3Config {

        /**
         * Bucket for managed release artifacts.
         *
         * @return bucket name
         */
        @WithName("bucket")
        Optional<String> bucket();

        /**
         * Object key prefix for managed release artifacts.
         *
         * @return object key prefix
         */
        @WithName("prefix")
        @WithDefault("tpf/releases")
        String prefix();

        /**
         * Optional S3 region override.
         *
         * @return region
         */
        @WithName("region")
        Optional<String> region();

        /**
         * Optional S3-compatible endpoint override, for example MinIO or LocalStack.
         *
         * @return endpoint URI
         */
        @WithName("endpoint-override")
        Optional<String> endpointOverride();

        /**
         * Enable path-style access for MinIO/LocalStack style endpoints.
         *
         * @return true when path-style access should be used
         */
        @WithName("path-style-access")
        @WithDefault("false")
        boolean pathStyleAccess();
    }

    /**
     * Transition worker execution settings, including admission limits and REST worker overrides.
     */
    interface WorkerConfig {

        /**
         * Execution mode for admitted transitions.
         *
         * @return worker execution mode
         */
        @WithName("execution-mode")
        @WithDefault("SAME_THREAD")
        TransitionWorkerExecutionMode executionMode();

        /**
         * Maximum admitted transitions per runtime instance.
         *
         * @return max in-flight transition count
         */
        @WithName("max-in-flight")
        @WithDefault("64")
        int maxInFlight();

        /**
         * Delay before re-enqueueing work when transition admission is saturated.
         *
         * @return saturated admission delay
         */
        @WithName("saturated-delay")
        @WithDefault("PT1S")
        Duration saturatedDelay();

        /**
         * Allowed Java package prefixes for application payload classes in transition envelopes.
         *
         * @return allowed application payload package prefixes
         */
        @WithName("allowed-payload-prefixes")
        @WithDefault("org.pipelineframework.")
        List<String> allowedPayloadPrefixes();

        /**
         * Optional deployable artifact id hosted by this worker runtime.
         *
         * @return artifact id when configured
         */
        @WithName("artifact-id")
        Optional<String> artifactId();

        /**
         * Optional deployable artifact digest hosted by this worker runtime.
         *
         * @return artifact digest when configured
         */
        @WithName("artifact-digest")
        Optional<String> artifactDigest();

        /**
         * REST transition worker settings.
         *
         * @return REST worker config
         */
        @WithName("rest")
        RestWorkerConfig rest();

        /**
         * gRPC transition worker settings.
         *
         * @return gRPC worker config
         */
        @WithName("grpc")
        GrpcWorkerConfig grpc();

        /**
         * SQS transition worker settings.
         *
         * @return SQS worker config
         */
        @WithName("sqs")
        SqsWorkerConfig sqs();

        /**
         * Worker lifecycle registry settings.
         *
         * @return lifecycle config
         */
        @WithName("lifecycle")
        WorkerLifecycleConfig lifecycle();
    }

    /**
     * Worker lifecycle registry settings.
     */
    interface WorkerLifecycleConfig {

        /**
         * Worker lifecycle provider name.
         *
         * @return memory or dynamo
         */
        @WithName("provider")
        @WithDefault("memory")
        String provider();

        /**
         * Maximum age since last heartbeat before a worker is considered stale.
         *
         * @return stale heartbeat duration
         */
        @WithName("stale-after")
        @WithDefault("PT2M")
        Duration staleAfter();
    }

    /**
     * REST transition worker settings.
     */
    interface RestWorkerConfig {

        /**
         * Optional remote worker base URL. Presence selects the REST worker client.
         *
         * @return remote worker base URL when configured
         */
        @WithName("base-url")
        Optional<String> baseUrl();

        /**
         * Worker execution path.
         *
         * @return REST worker path
         */
        @WithName("path")
        @WithDefault("/pipeline/worker/transitions/execute")
        String path();

        /**
         * Worker capability path.
         *
         * @return REST worker capability path
         */
        @WithName("capabilities-path")
        @WithDefault("/pipeline/worker/capabilities")
        String capabilitiesPath();

        /**
         * HTTP connect timeout.
         *
         * @return connect timeout
         */
        @WithName("connect-timeout")
        @WithDefault("PT2S")
        Duration connectTimeout();

        /**
         * HTTP request timeout.
         *
         * @return request timeout
         */
        @WithName("request-timeout")
        @WithDefault("PT30S")
        Duration requestTimeout();

        /**
         * Enables the local REST worker endpoint.
         *
         * @return true when the worker endpoint should accept requests
         */
        @WithName("server-enabled")
        @WithDefault("false")
        boolean serverEnabled();

        /**
         * Shared secret used to sign and verify REST transition worker requests.
         *
         * @return shared secret when configured
         */
        @WithName("shared-secret")
        Optional<String> sharedSecret();

        /**
         * Reference to the shared secret used to sign and verify REST transition worker requests.
         *
         * @return shared secret reference when configured
         */
        @WithName("shared-secret-ref")
        Optional<String> sharedSecretRef();

        /**
         * Maximum accepted timestamp skew for signed REST worker requests.
         *
         * @return signature timestamp tolerance
         */
        @WithName("signature-tolerance")
        @WithDefault("PT2M")
        Duration signatureTolerance();

        /**
         * True when a remote REST worker target is configured.
         *
         * @return true when REST worker client should be selected
         */
        default boolean isEnabled() {
            return baseUrl().filter(value -> !value.isBlank()).isPresent();
        }
    }

    /**
     * gRPC transition worker settings.
     */
    interface GrpcWorkerConfig {

        /**
         * Optional remote worker endpoint in host:port form. Presence selects the gRPC worker client.
         *
         * @return gRPC endpoint when configured
         */
        @WithName("endpoint")
        Optional<String> endpoint();

        /**
         * Uses plaintext gRPC channels for local/test workers.
         *
         * @return true when plaintext transport should be used
         */
        @WithName("plaintext")
        @WithDefault("false")
        boolean plaintext();

        /**
         * gRPC request timeout.
         *
         * @return request timeout
         */
        @WithName("request-timeout")
        @WithDefault("PT30S")
        Duration requestTimeout();

        /**
         * Maximum inbound gRPC worker response message size in bytes.
         *
         * @return max inbound message size
         */
        @WithName("max-inbound-message-size")
        @WithDefault("4194304")
        int maxInboundMessageSize();

        /**
         * Enables the local gRPC worker service.
         *
         * @return true when the worker service should accept requests
         */
        @WithName("server-enabled")
        @WithDefault("false")
        boolean serverEnabled();

        /**
         * Shared secret used to sign and verify gRPC transition worker requests.
         *
         * @return shared secret when configured
         */
        @WithName("shared-secret")
        Optional<String> sharedSecret();

        /**
         * Reference to the shared secret used to sign and verify gRPC transition worker requests.
         *
         * @return shared secret reference when configured
         */
        @WithName("shared-secret-ref")
        Optional<String> sharedSecretRef();

        /**
         * Maximum accepted timestamp skew for signed gRPC worker requests.
         *
         * @return signature timestamp tolerance
         */
        @WithName("signature-tolerance")
        @WithDefault("PT2M")
        Duration signatureTolerance();

        /**
         * True when a remote gRPC worker target is configured.
         *
         * @return true when gRPC worker client should be selected
         */
        default boolean isEnabled() {
            return endpoint().filter(value -> !value.isBlank()).isPresent();
        }
    }

    /**
     * SQS transition worker request/reply settings.
     */
    interface SqsWorkerConfig {

        /**
         * Queue URL that receives transition command envelopes.
         *
         * @return request queue URL when configured
         */
        @WithName("request-queue-url")
        Optional<String> requestQueueUrl();

        /**
         * Queue URL that receives transition result envelopes.
         *
         * @return response queue URL when configured
         */
        @WithName("response-queue-url")
        Optional<String> responseQueueUrl();

        /**
         * Static pipeline id hosted by the configured SQS worker queue.
         *
         * @return pipeline id when configured
         */
        @WithName("pipeline-id")
        Optional<String> pipelineId();

        /**
         * Static contract version hosted by the configured SQS worker queue.
         *
         * @return contract version when configured
         */
        @WithName("contract-version")
        Optional<String> contractVersion();

        /**
         * Static release version hosted by the configured SQS worker queue.
         *
         * @return release version when configured
         */
        @WithName("release-version")
        Optional<String> releaseVersion();

        /**
         * Static artifact id hosted by the configured SQS worker queue.
         *
         * @return artifact id when configured
         */
        @WithName("artifact-id")
        Optional<String> artifactId();

        /**
         * Static artifact digest hosted by the configured SQS worker queue.
         *
         * @return artifact digest when configured
         */
        @WithName("artifact-digest")
        Optional<String> artifactDigest();

        /**
         * Enables the local SQS transition worker poller.
         *
         * @return true when the worker poller should accept requests
         */
        @WithName("server-enabled")
        @WithDefault("false")
        boolean serverEnabled();

        /**
         * SQS worker request timeout.
         *
         * @return request timeout
         */
        @WithName("request-timeout")
        @WithDefault("PT30S")
        Duration requestTimeout();

        /**
         * Optional delay before the SQS worker poller starts consuming requests.
         *
         * @return poller start delay
         */
        @WithName("poll-start-delay")
        @WithDefault("PT0S")
        Duration pollStartDelay();

        /**
         * Visibility timeout for claimed worker request and response messages.
         *
         * @return visibility timeout
         */
        @WithName("visibility-timeout")
        @WithDefault("PT30S")
        Duration visibilityTimeout();

        /**
         * Shared secret used to sign and verify SQS transition worker messages.
         *
         * @return shared secret when configured
         */
        @WithName("shared-secret")
        Optional<String> sharedSecret();

        /**
         * Reference to the shared secret used to sign and verify SQS transition worker messages.
         *
         * @return shared secret reference when configured
         */
        @WithName("shared-secret-ref")
        Optional<String> sharedSecretRef();

        /**
         * Maximum accepted timestamp skew for signed SQS worker messages.
         *
         * @return signature timestamp tolerance
         */
        @WithName("signature-tolerance")
        @WithDefault("PT2M")
        Duration signatureTolerance();

        /**
         * True when a remote SQS worker target is configured.
         *
         * @return true when SQS worker client should be selected
         */
        default boolean isEnabled() {
            return requestQueueUrl().filter(value -> !value.isBlank()).isPresent();
        }
    }
}
