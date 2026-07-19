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
    CircuitPolicyResolver(PipelineResilienceConfig resilienceConfig) {
        this(toSettings(resilienceConfig));
    }

    CircuitPolicyResolver(Map<String, CircuitSettings> settings) {
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
            if (policy.requiredScope() != CircuitScope.LOCAL_PROCESS) {
                throw new IllegalArgumentException(
                    "Circuit '" + key + "' requires " + policy.requiredScope()
                        + " but only " + CircuitScope.LOCAL_PROCESS + " is available");
            }
            Optional<String> configuredIdentity = configured.identity();
            if (isTransitionWorkerKey(key)) {
                throw new IllegalArgumentException(
                    "Local-process circuits are not supported for durable transition-worker dispatch");
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
            value.identity())));
        return settings;
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
    Optional<String> identity
) {
    CircuitSettings {
        Objects.requireNonNull(scope, "scope must not be null");
        Objects.requireNonNull(failureWindow, "failureWindow must not be null");
        Objects.requireNonNull(openDuration, "openDuration must not be null");
        Objects.requireNonNull(halfOpenRetryDelay, "halfOpenRetryDelay must not be null");
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
            halfOpenRetryDelay);
    }
}
