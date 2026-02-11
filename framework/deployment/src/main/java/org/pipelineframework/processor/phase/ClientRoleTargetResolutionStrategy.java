package org.pipelineframework.processor.phase;

import java.util.Set;
import java.util.Objects;

import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.TransportMode;

/**
 * Target resolution strategy for client roles.
 */
public class ClientRoleTargetResolutionStrategy implements TargetResolutionStrategy {

    @Override
    public Set<GenerationTarget> resolve(TransportMode transportMode) {
        Objects.requireNonNull(transportMode, "transportMode must not be null");
        return switch (transportMode) {
            case REST -> Set.of(GenerationTarget.REST_CLIENT_STEP);
            case LOCAL -> Set.of(GenerationTarget.LOCAL_CLIENT_STEP);
            case GRPC -> Set.of(GenerationTarget.CLIENT_STEP);
        };
    }
}
