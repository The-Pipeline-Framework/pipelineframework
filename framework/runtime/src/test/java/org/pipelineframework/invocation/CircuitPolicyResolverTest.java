package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

class CircuitPolicyResolverTest {

    @Test
    void appliesTheGlobalDefaultToEligibleManagedBoundaries() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(defaults(), Map.of(), false);

        ResolvedCircuitPolicy resolved = resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess")).orElseThrow();

        assertEquals("grpc:pricing.remoteProcess", resolved.identity().value());
        assertEquals(CircuitScope.LOCAL_PROCESS, resolved.policy().requiredScope());
        assertEquals(CircuitPolicySource.GLOBAL_DEFAULT, resolved.source());
    }

    @Test
    void globalCompatibilityOptOutDisablesEligibleManagedBoundaries() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(disabledDefaults(), Map.of(), false);

        assertEquals(Optional.empty(), resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess")));
    }

    @Test
    void localGlobalDefaultDoesNotProtectDurableTransitionWorkerDispatch() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(defaults(), Map.of(), false);

        assertEquals(Optional.empty(), resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "transition-worker.execute")));
    }

    @Test
    void exactBoundaryOverrideCanUseSharedScopeAlongsideLocalDefault() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(defaults(), Map.of(
            "grpc:pricing.remoteProcess", new CircuitSettings(
                true,
                CircuitScope.SHARED_DEPENDENCY,
                2,
                Duration.ofMinutes(1),
                Duration.ofSeconds(30),
                1,
                Duration.ofSeconds(1),
                Optional.of("pricing-service"))), true);

        ResolvedCircuitPolicy shared = resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess")).orElseThrow();
        ResolvedCircuitPolicy local = resolver.resolve(
            new TransportBoundaryDescriptor("rest", "catalog.process")).orElseThrow();

        assertEquals(CircuitScope.SHARED_DEPENDENCY, shared.policy().requiredScope());
        assertEquals(CircuitPolicySource.BOUNDARY_OVERRIDE, shared.source());
        assertEquals(CircuitScope.LOCAL_PROCESS, local.policy().requiredScope());
    }

    @Test
    void usesStableOperatorBoundaryKeyAsTheDefaultIdentity() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", enabled(Optional.empty())));

        ResolvedCircuitPolicy resolved = resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess")).orElseThrow();

        assertEquals("grpc:pricing.remoteProcess", resolved.identity().value());
    }

    @Test
    void treatsBlankExplicitIdentityAsTheDefaultBoundaryIdentity() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", enabled(Optional.of("   "))));

        ResolvedCircuitPolicy resolved = resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess")).orElseThrow();

        assertEquals("grpc:pricing.remoteProcess", resolved.identity().value());
    }

    @Test
    void allowsCompatibleBoundariesToShareAnExplicitIdentity() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", enabled(Optional.of("pricing-service")),
            "rest:pricing.process", enabled(Optional.of("pricing-service"))));

        assertEquals("pricing-service", resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess")).orElseThrow().identity().value());
        assertEquals("pricing-service", resolver.resolve(
            new TransportBoundaryDescriptor("rest", "pricing.process")).orElseThrow().identity().value());
    }

    @Test
    void rejectsLocalCircuitForDurableTransitionWorkerWithoutLogicalTargetIdentity() {
        assertThrows(IllegalArgumentException.class, () -> new CircuitPolicyResolver(Map.of(
            "grpc:transition-worker.execute", enabled(Optional.empty()))));
    }

    @Test
    void rejectsLocalCircuitForDurableTransitionWorkerEvenWithLogicalTargetIdentity() {
        assertThrows(IllegalArgumentException.class, () -> new CircuitPolicyResolver(Map.of(
            "grpc:transition-worker.execute", enabled(Optional.of("payment-worker")))));
    }

    @Test
    void rejectsIncompatiblePoliciesForOneIdentity() {
        assertThrows(IllegalArgumentException.class, () -> new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", enabled(Optional.of("pricing-service")),
            "rest:pricing.process", new CircuitSettings(
                true,
                CircuitScope.LOCAL_PROCESS,
                2,
                Duration.ofMinutes(1),
                Duration.ofSeconds(30),
                1,
                Duration.ofSeconds(1),
                Optional.of("pricing-service")))));
    }

    @Test
    void rejectsSharedScopeBeforeAnyInvocation() {
        assertThrows(IllegalArgumentException.class, () -> new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", new CircuitSettings(
                true,
                CircuitScope.SHARED_DEPENDENCY,
                1,
                Duration.ofMinutes(1),
                Duration.ofSeconds(30),
                1,
                Duration.ofSeconds(1),
                Optional.empty()))));
    }

    @Test
    void ignoresDisabledSharedScopeCircuit() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", new CircuitSettings(
                false,
                CircuitScope.SHARED_DEPENDENCY,
                1,
                Duration.ofMinutes(1),
                Duration.ofSeconds(30),
                1,
                Duration.ofSeconds(1),
                Optional.empty())));

        assertEquals(Optional.empty(), resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess")));
    }

    @Test
    void legacySettingsKeepTheProbeLeaseIndependentFromOpenDuration() {
        CircuitPolicyResolver resolver = new CircuitPolicyResolver(Map.of(
            "grpc:pricing.remoteProcess", new CircuitSettings(
                true,
                CircuitScope.LOCAL_PROCESS,
                1,
                Duration.ofMinutes(1),
                Duration.ofSeconds(7),
                1,
                Duration.ofSeconds(1),
                Optional.empty())));

        assertEquals(Duration.ofSeconds(30), resolver.resolve(
            new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess"))
            .orElseThrow().policy().halfOpenProbeLeaseDuration());
    }

    private static CircuitSettings enabled(Optional<String> identity) {
        return new CircuitSettings(
            true,
            CircuitScope.LOCAL_PROCESS,
            1,
            Duration.ofMinutes(1),
            Duration.ofSeconds(30),
            1,
            Duration.ofSeconds(1),
            identity);
    }

    private static CircuitSettings defaults() {
        return enabled(Optional.empty());
    }

    private static CircuitSettings disabledDefaults() {
        return new CircuitSettings(
            false,
            CircuitScope.LOCAL_PROCESS,
            1,
            Duration.ofMinutes(1),
            Duration.ofSeconds(30),
            1,
            Duration.ofSeconds(1),
            Optional.empty());
    }
}
