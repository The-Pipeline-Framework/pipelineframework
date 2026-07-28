package org.pipelineframework.representation.spi;

import java.util.Optional;

/** A provider's exclusive request to own a configured boundary. */
public record BoundaryClaim(String providerKey, String bindingId, String generatedFacadeTypeName,
                            Optional<ProviderStepContract> stepContract) {
    public BoundaryClaim {
        if (providerKey == null || providerKey.isBlank() || bindingId == null || bindingId.isBlank()
            || generatedFacadeTypeName == null || generatedFacadeTypeName.isBlank() || stepContract == null) {
            throw new IllegalArgumentException("boundary claim fields must not be blank");
        }
        providerKey = providerKey.trim();
        bindingId = bindingId.trim();
        generatedFacadeTypeName = generatedFacadeTypeName.trim();
    }

    public BoundaryClaim(String providerKey, String bindingId, String generatedFacadeTypeName) {
        this(providerKey, bindingId, generatedFacadeTypeName, Optional.empty());
    }
}
