package org.pipelineframework.invocation;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.config.PipelineResilienceConfig;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitPolicy;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

/**
 * Resolves explicitly configured, local circuit policies for stable transport boundaries.
 */
@Startup
@ApplicationScoped
final class CircuitPolicyResolver {
    private static final String TRANSITION_WORKER_TARGET = "transition-worker.execute";

    private final Map<String, ResolvedCircuitPolicy> policies;

    @Inject
    CircuitPolicyResolver(
        PipelineResilienceConfig resilienceConfig,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this(toSettings(resilienceConfig), sharedConfigured(resilienceConfig), Optional.of(orchestratorConfig));
    }

    CircuitPolicyResolver(Map<String, CircuitSettings> settings) {
        this(settings, false, Optional.empty());
    }

    private CircuitPolicyResolver(
        Map<String, CircuitSettings> settings,
        boolean sharedConfigured,
        Optional<PipelineOrchestratorConfig> orchestratorConfig
    ) {
        Objects.requireNonNull(settings, "settings must not be null");
        Map<String, ResolvedCircuitPolicy> resolved = new LinkedHashMap<>();
        Map<CircuitIdentity, CircuitPolicy> policiesByIdentity = new LinkedHashMap<>();
        settings.forEach((boundaryKey, setting) -> {
            String key = requireText(boundaryKey, "circuit boundary key");
            CircuitSettings configured = Objects.requireNonNull(setting, "circuit setting must not be null");
            if (!configured.enabled()) {
                return;
            }
            CircuitPolicy policy = configured.policy();
            if (policy.requiredScope() == CircuitScope.SHARED_DEPENDENCY && !sharedConfigured) {
                throw new IllegalArgumentException(
                    "Circuit '" + key + "' requires pipeline.resilience.shared.dynamo-table");
            }
            Optional<String> configuredIdentity = configured.identity();
            if (policy.requiredScope() == CircuitScope.LOCAL_PROCESS && isTransitionWorkerKey(key)) {
                throw new IllegalArgumentException(
                    "Local-process circuits are not supported for durable transition-worker dispatch");
            }
            if (policy.requiredScope() == CircuitScope.SHARED_DEPENDENCY && configuredIdentity.isEmpty()) {
                throw new IllegalArgumentException(
                    "Shared circuit '" + key + "' requires an explicit identity");
            }
            if (policy.requiredScope() == CircuitScope.SHARED_DEPENDENCY && isTransitionWorkerKey(key)) {
                requireFiniteCircuitDeferral(orchestratorConfig);
            }
            CircuitIdentity identity = new CircuitIdentity(configuredIdentity.orElse(key));
            CircuitPolicy previous = policiesByIdentity.putIfAbsent(identity, policy);
            if (previous != null && !previous.equals(policy)) {
                throw new IllegalArgumentException(
                    "Circuit identity '" + identity.value() + "' has incompatible policies");
            }
            resolved.put(key, new ResolvedCircuitPolicy(identity, policy));
        });
        policies = Map.copyOf(resolved);
    }

    static CircuitPolicyResolver disabled() {
        return new CircuitPolicyResolver(Map.of());
    }

    Optional<ResolvedCircuitPolicy> resolve(TransportBoundaryDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        return Optional.ofNullable(policies.get(boundaryKey(descriptor)));
    }

    static String boundaryKey(TransportBoundaryDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        return descriptor.protocol() + ":" + descriptor.target();
    }

    private static boolean isTransitionWorkerKey(String key) {
        return key.endsWith(":" + TRANSITION_WORKER_TARGET);
    }

    private static Map<String, CircuitSettings> toSettings(PipelineResilienceConfig resilienceConfig) {
        Objects.requireNonNull(resilienceConfig, "resilienceConfig must not be null");
        Map<String, CircuitSettings> settings = new LinkedHashMap<>();
        resilienceConfig.circuit().forEach((key, value) -> settings.put(key, new CircuitSettings(
            value.enabled(),
            value.scope(),
            value.failureThreshold(),
            value.failureWindow(),
            value.openDuration(),
            value.halfOpenMaxPermits(),
            value.halfOpenRetryDelay(),
            value.halfOpenProbeLeaseDuration(),
            value.identity())));
        return settings;
    }

    private static boolean sharedConfigured(PipelineResilienceConfig resilienceConfig) {
        Objects.requireNonNull(resilienceConfig, "resilienceConfig must not be null");
        boolean sharedRequested = resilienceConfig.circuit().values().stream()
            .anyMatch(value -> value.enabled() && value.scope() == CircuitScope.SHARED_DEPENDENCY);
        if (!sharedRequested) {
            return true;
        }
        return resilienceConfig.shared().dynamoTable().map(String::trim).filter(value -> !value.isEmpty()).isPresent();
    }

    private static void requireFiniteCircuitDeferral(Optional<PipelineOrchestratorConfig> config) {
        if (config.filter(value -> value.mode() == OrchestratorMode.QUEUE_ASYNC)
            .flatMap(PipelineOrchestratorConfig::maxCircuitDeferral)
            .filter(value -> !value.isZero() && !value.isNegative())
            .isEmpty()) {
            throw new IllegalArgumentException(
                "Shared transition-worker circuits require finite pipeline.orchestrator.max-circuit-deferral in QUEUE_ASYNC mode");
        }
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return trimmed;
    }
}

record ResolvedCircuitPolicy(CircuitIdentity identity, CircuitPolicy policy) {
    ResolvedCircuitPolicy {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(policy, "policy must not be null");
    }
}

record CircuitSettings(
    boolean enabled,
    CircuitScope scope,
    int failureThreshold,
    Duration failureWindow,
    Duration openDuration,
    int halfOpenMaxPermits,
    Duration halfOpenRetryDelay,
    Duration halfOpenProbeLeaseDuration,
    Optional<String> identity
) {
    private static final Duration DEFAULT_HALF_OPEN_PROBE_LEASE_DURATION = Duration.ofSeconds(30);

    CircuitSettings(
        boolean enabled,
        CircuitScope scope,
        int failureThreshold,
        Duration failureWindow,
        Duration openDuration,
        int halfOpenMaxPermits,
        Duration halfOpenRetryDelay,
        Optional<String> identity
    ) {
        this(enabled, scope, failureThreshold, failureWindow, openDuration, halfOpenMaxPermits,
            halfOpenRetryDelay, DEFAULT_HALF_OPEN_PROBE_LEASE_DURATION, identity);
    }
    CircuitSettings {
        Objects.requireNonNull(scope, "scope must not be null");
        Objects.requireNonNull(failureWindow, "failureWindow must not be null");
        Objects.requireNonNull(openDuration, "openDuration must not be null");
        Objects.requireNonNull(halfOpenRetryDelay, "halfOpenRetryDelay must not be null");
        Objects.requireNonNull(halfOpenProbeLeaseDuration, "halfOpenProbeLeaseDuration must not be null");
        identity = Objects.requireNonNull(identity, "identity must not be null")
            .map(String::trim)
            .filter(value -> !value.isEmpty());
    }

    CircuitPolicy policy() {
        return new CircuitPolicy(
            scope,
            failureThreshold,
            failureWindow,
            openDuration,
            halfOpenMaxPermits,
            halfOpenRetryDelay,
            halfOpenProbeLeaseDuration);
    }
}
