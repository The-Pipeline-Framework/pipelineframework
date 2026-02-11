package org.pipelineframework.processor.phase;

import java.util.Set;
import java.util.Objects;

import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.TransportMode;

/**
 * Target resolution strategy for server roles.
 */
public class ServerRoleTargetResolutionStrategy implements TargetResolutionStrategy {

    @Override
    public Set<GenerationTarget> resolve(TransportMode transportMode) {
        Objects.requireNonNull(transportMode, "transportMode must not be null");
        return switch (transportMode) {
            case REST -> Set.of(GenerationTarget.REST_RESOURCE);
            case LOCAL -> Set.of(GenerationTarget.GRPC_SERVICE_SIDE_EFFECT_ONLY);
            case GRPC -> Set.of(GenerationTarget.GRPC_SERVICE);
        };
    }
}
