package org.pipelineframework.blocks.openapi.mapping;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public final class FinishOpenApiRepresentationMappingService
    implements ReactiveService<OpenApiRepresentationMappingProposal, OpenApiRepresentationMappingProposal> {
    @Override
    public Uni<OpenApiRepresentationMappingProposal> process(OpenApiRepresentationMappingProposal proposal) {
        return Uni.createFrom().item(proposal);
    }
}
