package org.pipelineframework.invocation;

import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.pipelineframework.runtime.core.resilience.CircuitBreakerListener;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitOpen;
import org.pipelineframework.runtime.core.resilience.CircuitScope;
import org.pipelineframework.runtime.core.resilience.CircuitStateTransition;

/**
 * OpenTelemetry adapter for circuit admission and state transitions.
 */
@ApplicationScoped
final class CircuitTelemetry implements CircuitBreakerListener {
    private static final AttributeKey<String> IDENTITY = AttributeKey.stringKey("tpf.circuit.identity");
    private static final AttributeKey<String> SCOPE = AttributeKey.stringKey("tpf.circuit.scope");
    private static final AttributeKey<String> PROTOCOL = AttributeKey.stringKey("tpf.transport.protocol");
    private static final AttributeKey<String> TARGET = AttributeKey.stringKey("tpf.transport.target");
    private static final AttributeKey<String> ADMISSION = AttributeKey.stringKey("tpf.circuit.admission");
    private static final AttributeKey<String> TRANSITION = AttributeKey.stringKey("tpf.circuit.transition");
    private final LongCounter admissions;
    private final LongCounter transitions;

    @Inject
    CircuitTelemetry(OpenTelemetry openTelemetry) {
        this(Objects.requireNonNull(openTelemetry, "openTelemetry must not be null")
            .getMeter("org.pipelineframework.resilience"));
    }

    CircuitTelemetry(Meter meter) {
        Meter localMeter = Objects.requireNonNull(meter, "meter must not be null");
        admissions = localMeter.counterBuilder("tpf.circuit.admissions").build();
        transitions = localMeter.counterBuilder("tpf.circuit.transitions").build();
    }

    void permitted(TransportBoundaryDescriptor descriptor, ResolvedCircuitPolicy circuit) {
        admissions.add(1, admissionAttributes(descriptor, circuit.identity(), circuit.policy().requiredScope(), "permitted"));
    }

    void rejected(TransportBoundaryDescriptor descriptor, CircuitOpen open) {
        admissions.add(1, admissionAttributes(descriptor, open.identity(), open.scope(), "rejected"));
    }

    @Override
    public void onTransition(CircuitIdentity identity, CircuitScope scope, CircuitStateTransition transition) {
        transitions.add(1, Attributes.builder()
            .put(IDENTITY, identity.value())
            .put(SCOPE, scope.name())
            .put(TRANSITION, transition.name())
            .build());
    }

    private static Attributes admissionAttributes(
        TransportBoundaryDescriptor descriptor,
        CircuitIdentity identity,
        CircuitScope scope,
        String admission
    ) {
        return Attributes.builder()
            .put(IDENTITY, identity.value())
            .put(SCOPE, scope.name())
            .put(PROTOCOL, descriptor.protocol())
            .put(TARGET, descriptor.target())
            .put(ADMISSION, admission)
            .build();
    }
}
