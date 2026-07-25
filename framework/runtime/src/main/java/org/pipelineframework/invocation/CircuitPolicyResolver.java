package org.pipelineframework.invocation;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineCircuitDefaultsConfig;
import org.pipelineframework.config.PipelineCircuitOverridesConfig;
import org.pipelineframework.config.PipelineResilienceConfig;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitPolicy;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

/**
 * Resolves the global circuit safety floor and advanced exact-boundary overrides.
 */
@Startup
@ApplicationScoped
final class CircuitPolicyResolver {
    private static final Logger LOG = Logger.getLogger(CircuitPolicyResolver.class);
    private static final String TRANSITION_WORKER_TARGET = "transition-worker.execute";
    private static final List<String> DEFAULT_PROPERTY_KEYS = List.of(
        "pipeline.defaults.circuit.enabled",
        "pipeline.defaults.circuit.failure-threshold",
        "pipeline.defaults.circuit.failure-window",
        "pipeline.defaults.circuit.open-duration",
        "pipeline.defaults.circuit.half-open-max-permits",
        "pipeline.defaults.circuit.half-open-retry-delay",
        "pipeline.defaults.circuit.half-open-probe-lease-duration");

    private final Map<String, Optional<ResolvedCircuitPolicy>> overrides;
    private final Optional<CircuitSettings> defaults;
    private final CircuitPolicySource defaultSource;
    private final Optional<PipelineOrchestratorConfig> orchestratorConfig;

    @Inject
    CircuitPolicyResolver(
        PipelineResilienceConfig resilienceConfig,
        PipelineCircuitDefaultsConfig circuitDefaults,
        PipelineCircuitOverridesConfig circuitOverrides,
        PipelineOrchestratorConfig orchestratorConfig,
        Config config
    ) {
        this(
            toSettings(resilienceConfig, circuitDefaults, circuitOverrides),
            Optional.of(defaultSettings(circuitDefaults, resilienceConfig.defaultCircuitScope())),
            defaultSource(config),
            sharedConfigured(
                resilienceConfig,
                defaultSettings(circuitDefaults, resilienceConfig.defaultCircuitScope()),
                toSettings(resilienceConfig, circuitDefaults, circuitOverrides)),
            Optional.of(orchestratorConfig));
    }

    CircuitPolicyResolver(Map<String, CircuitSettings> settings) {
        this(settings, Optional.empty(), CircuitPolicySource.BUILTIN, false, Optional.empty());
    }

    CircuitPolicyResolver(CircuitSettings defaults, Map<String, CircuitSettings> settings, boolean sharedConfigured) {
        this(settings, Optional.of(defaults), CircuitPolicySource.GLOBAL_DEFAULT, sharedConfigured, Optional.empty());
    }

    private CircuitPolicyResolver(
        Map<String, CircuitSettings> settings,
        Optional<CircuitSettings> defaults,
        CircuitPolicySource defaultSource,
        boolean sharedConfigured,
        Optional<PipelineOrchestratorConfig> orchestratorConfig
    ) {
        Objects.requireNonNull(settings, "settings must not be null");
        this.defaults = Objects.requireNonNull(defaults, "defaults must not be null");
        this.defaultSource = Objects.requireNonNull(defaultSource, "defaultSource must not be null");
        this.orchestratorConfig = Objects.requireNonNull(orchestratorConfig, "orchestratorConfig must not be null");

        if (this.defaults.filter(CircuitSettings::enabled).isPresent()) {
            requireScopeAvailable("pipeline.defaults.circuit", this.defaults.orElseThrow(), sharedConfigured);
        }

        Map<String, Optional<ResolvedCircuitPolicy>> resolved = new LinkedHashMap<>();
        Map<CircuitIdentity, CircuitPolicy> policiesByIdentity = new LinkedHashMap<>();
        settings.forEach((boundaryKey, setting) -> {
            String key = requireText(boundaryKey, "circuit boundary key");
            CircuitSettings configured = Objects.requireNonNull(setting, "circuit setting must not be null");
            if (!configured.enabled()) {
                resolved.put(key, Optional.empty());
                return;
            }
            requireScopeAvailable(key, configured, sharedConfigured);
            if (configured.policy().requiredScope() == CircuitScope.LOCAL_PROCESS && isTransitionWorkerKey(key)) {
                throw new IllegalArgumentException(
                    "Local-process circuits are not supported for durable transition-worker dispatch");
            }
            Optional<String> configuredIdentity = configured.identity();
            if (configured.policy().requiredScope() == CircuitScope.SHARED_DEPENDENCY && configuredIdentity.isEmpty()) {
                throw new IllegalArgumentException(
                    "Shared circuit '" + key + "' requires an explicit identity");
            }
            if (configured.policy().requiredScope() == CircuitScope.SHARED_DEPENDENCY && isTransitionWorkerKey(key)) {
                requireFiniteCircuitDeferral(this.orchestratorConfig);
            }
            CircuitIdentity identity = new CircuitIdentity(configuredIdentity.orElse(key));
            CircuitPolicy policy = configured.policy();
            CircuitPolicy previous = policiesByIdentity.putIfAbsent(identity, policy);
            if (previous != null && !previous.equals(policy)) {
                throw new IllegalArgumentException(
                    "Circuit identity '" + identity.value() + "' has incompatible policies");
            }
            resolved.put(key, Optional.of(new ResolvedCircuitPolicy(identity, policy, CircuitPolicySource.BOUNDARY_OVERRIDE)));
        });
        overrides = Map.copyOf(resolved);
        LOG.infof(
            "Circuit admission defaults: source=%s enabled=%s defaultScope=%s exactBoundaryOverrides=%d",
            this.defaultSource,
            this.defaults.map(CircuitSettings::enabled).orElse(false),
            this.defaults.map(CircuitSettings::scope).map(CircuitScope::name).orElse("none"),
            overrides.size());
        overrides.forEach((boundary, policy) -> policy.ifPresent(resolvedPolicy -> LOG.infof(
            "Circuit admission boundary override: boundary=%s identity=%s scope=%s source=%s",
            boundary,
            resolvedPolicy.identity().value(),
            resolvedPolicy.policy().requiredScope(),
            resolvedPolicy.source())));
    }

    static CircuitPolicyResolver disabled() {
        return new CircuitPolicyResolver(Map.of());
    }

    Optional<ResolvedCircuitPolicy> resolve(TransportBoundaryDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        String boundaryKey = boundaryKey(descriptor);
        Optional<ResolvedCircuitPolicy> configured = overrides.get(boundaryKey);
        if (configured != null) {
            return configured;
        }
        return defaults.filter(CircuitSettings::enabled)
            .filter(defaults -> isDefaultEligible(boundaryKey, defaults))
            .map(defaults -> new ResolvedCircuitPolicy(
                new CircuitIdentity(boundaryKey), defaults.policy(), defaultSource));
    }

    static String boundaryKey(TransportBoundaryDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        return descriptor.protocol() + ":" + descriptor.target();
    }

    private boolean isDefaultEligible(String boundaryKey, CircuitSettings settings) {
        if (!isTransitionWorkerKey(boundaryKey)) {
            return true;
        }
        return settings.scope() == CircuitScope.SHARED_DEPENDENCY
            && hasFiniteCircuitDeferral(orchestratorConfig);
    }

    private static boolean isTransitionWorkerKey(String key) {
        return key.endsWith(":" + TRANSITION_WORKER_TARGET);
    }

    private static CircuitSettings defaultSettings(
        PipelineCircuitDefaultsConfig defaults,
        CircuitScope scope
    ) {
        Objects.requireNonNull(defaults, "circuit defaults must not be null");
        return new CircuitSettings(
            defaults.enabled(),
            scope,
            defaults.failureThreshold(),
            defaults.failureWindow(),
            defaults.openDuration(),
            defaults.halfOpenMaxPermits(),
            defaults.halfOpenRetryDelay(),
            defaults.halfOpenProbeLeaseDuration(),
            Optional.empty());
    }

    private static Map<String, CircuitSettings> toSettings(
        PipelineResilienceConfig resilienceConfig,
        PipelineCircuitDefaultsConfig defaults,
        PipelineCircuitOverridesConfig overrides
    ) {
        Objects.requireNonNull(resilienceConfig, "resilienceConfig must not be null");
        Objects.requireNonNull(defaults, "circuit defaults must not be null");
        Objects.requireNonNull(overrides, "circuit overrides must not be null");
        Map<String, CircuitSettings> settings = new LinkedHashMap<>();
        overrides.circuit().forEach((key, value) -> settings.put(key, new CircuitSettings(
            value.enabled().orElse(defaults.enabled()),
            value.scope().orElse(resilienceConfig.defaultCircuitScope()),
            value.failureThreshold().orElse(defaults.failureThreshold()),
            value.failureWindow().orElse(defaults.failureWindow()),
            value.openDuration().orElse(defaults.openDuration()),
            value.halfOpenMaxPermits().orElse(defaults.halfOpenMaxPermits()),
            value.halfOpenRetryDelay().orElse(defaults.halfOpenRetryDelay()),
            value.halfOpenProbeLeaseDuration().orElse(defaults.halfOpenProbeLeaseDuration()),
            value.identity())));
        return settings;
    }

    private static CircuitPolicySource defaultSource(Config config) {
        Objects.requireNonNull(config, "config must not be null");
        return DEFAULT_PROPERTY_KEYS.stream()
            .anyMatch(key -> config.getOptionalValue(key, String.class).isPresent())
            ? CircuitPolicySource.GLOBAL_DEFAULT
            : CircuitPolicySource.BUILTIN;
    }

    private static boolean sharedConfigured(
        PipelineResilienceConfig resilienceConfig,
        CircuitSettings defaults,
        Map<String, CircuitSettings> settings
    ) {
        Objects.requireNonNull(resilienceConfig, "resilienceConfig must not be null");
        boolean sharedRequested = (defaults.enabled() && defaults.scope() == CircuitScope.SHARED_DEPENDENCY)
            || settings.values().stream().anyMatch(value -> value.enabled()
                && value.scope() == CircuitScope.SHARED_DEPENDENCY);
        if (!sharedRequested) {
            return true;
        }
        return resilienceConfig.shared().dynamoTable().map(String::trim).filter(value -> !value.isEmpty()).isPresent();
    }

    private static void requireScopeAvailable(String name, CircuitSettings settings, boolean sharedConfigured) {
        if (settings.scope() == CircuitScope.SHARED_DEPENDENCY && !sharedConfigured) {
            throw new IllegalArgumentException(
                "Circuit '" + name + "' requires pipeline.resilience.shared.dynamo-table");
        }
    }

    private static boolean hasFiniteCircuitDeferral(Optional<PipelineOrchestratorConfig> config) {
        return config.filter(value -> value.mode() == OrchestratorMode.QUEUE_ASYNC)
            .flatMap(PipelineOrchestratorConfig::maxCircuitDeferral)
            .filter(value -> !value.isZero() && !value.isNegative())
            .isPresent();
    }

    private static void requireFiniteCircuitDeferral(Optional<PipelineOrchestratorConfig> config) {
        if (!hasFiniteCircuitDeferral(config)) {
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

enum CircuitPolicySource {
    BUILTIN,
    GLOBAL_DEFAULT,
    BOUNDARY_OVERRIDE
}

record ResolvedCircuitPolicy(CircuitIdentity identity, CircuitPolicy policy, CircuitPolicySource source) {
    ResolvedCircuitPolicy {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(policy, "policy must not be null");
        Objects.requireNonNull(source, "source must not be null");
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
