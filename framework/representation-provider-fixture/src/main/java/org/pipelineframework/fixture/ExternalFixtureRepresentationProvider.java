package org.pipelineframework.fixture;

import java.util.Optional;
import java.util.Set;

import org.pipelineframework.representation.spi.ProviderMetadata;
import org.pipelineframework.representation.spi.ProviderSchemaFragment;
import org.pipelineframework.representation.spi.RepresentationProvider;

/** Separate-JAR fixture used to prove host discovery rather than direct provider construction. */
public final class ExternalFixtureRepresentationProvider implements RepresentationProvider {
    @Override
    public ProviderMetadata metadata() {
        return new ProviderMetadata("external-fixture", Set.of(), Set.of("fixture"));
    }

    @Override
    public ProviderSchemaFragment schema() {
        return new ProviderSchemaFragment("external-fixture", Optional.of("{\"type\":\"object\"}"),
            Optional.of("{\"type\":\"object\"}"), Optional.of("External provider fixture."));
    }
}
