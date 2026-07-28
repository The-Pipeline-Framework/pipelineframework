package org.pipelineframework.processor.phase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.RepresentationMapping;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.RepresentationScope;

class RepresentationProviderPreparationPhaseTest {

    @Test
    void claimedBoundaryValidatesItsDeclaredTypeMappingWithoutGlobalConfiguration() {
        RepresentationMapping mapping = new RepresentationMapping("opencsv", "PaymentRecord",
            Optional.of("example.PaymentRow"), Optional.of("example.PaymentMapper"), Map.of("separator", ","));

        var configuration = RepresentationProviderPreparationPhase.typeConfiguration(
            new BoundaryClaim("opencsv", "Read Payments", "example.PaymentReader", Optional.empty()), mapping);

        assertEquals(RepresentationScope.TYPE, configuration.scope());
        assertEquals("opencsv", configuration.providerKey());
        assertEquals(Map.of("separator", ","), configuration.options());
    }
}
