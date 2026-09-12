package org.pipelineframework.branching;

import java.util.Objects;

/**
 * Stable semantic identity for one declared union alternative.
 *
 * <p>This is observability and durable-contract metadata. Branch applicability
 * remains based on the concrete runtime payload type.</p>
 */
public record BranchVariantIdentity(
    String unionName,
    String discriminator,
    String payloadContract
) {

    public BranchVariantIdentity {
        unionName = requireNonBlank(unionName, "unionName");
        discriminator = requireNonBlank(discriminator, "discriminator");
        payloadContract = requireNonBlank(payloadContract, "payloadContract");
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name + " is required");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
