package org.pipelineframework.processor.util;

import java.util.List;
import java.util.Collection;
import java.util.Optional;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.phase.PipelineTargetResolutionPhase;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;
import org.pipelineframework.representation.spi.ProviderCapability;

/**
 * The deliberately narrow compiler eligibility rule for producer-owned durable stream regions.
 *
 * <p>This is not a general stream IR: one resumable {@code UNARY_STREAMING} producer must feed
 * its immediately following scalar Await. A terminal fact is emitted only when the remaining
 * suffix is entirely scalar and contains no additional durable boundary.
 */
public final class ResumableSourceContinuationEligibility {

    private ResumableSourceContinuationEligibility() {
    }

    /**
     * Resolves the provider binding after provider generation has replaced the model's service
     * class with its generated facade. Boundary-map keys are authored YAML step names, not the
     * model service name, so lookup by service name would silently lose the CSV binding.
     */
    public static Optional<ResolvedProviderBoundary> providerBoundary(
        PipelineStepModel producer,
        Collection<ResolvedProviderBoundary> boundaries
    ) {
        if (producer == null || boundaries == null) {
            return Optional.empty();
        }
        ResolvedProviderBoundary match = null;
        for (ResolvedProviderBoundary boundary : boundaries) {
            boolean matchesFacade = boundary.claim().generatedFacadeTypeName()
                .equals(producer.serviceClassName().canonicalName());
            boolean matchesOriginal = boundary.boundary().serviceTypeName()
                .equals(producer.serviceClassName().canonicalName());
            boolean matchesLegacyName = boundary.boundary().stepName().equals(producer.serviceName());
            if (!matchesFacade && !matchesOriginal && !matchesLegacyName) {
                continue;
            }
            if (match != null) {
                throw new IllegalStateException("Multiple provider boundaries match producer " + producer.serviceName());
            }
            match = boundary;
        }
        return Optional.ofNullable(match);
    }

    public static Optional<Candidate> candidate(
        List<PipelineStepModel> orderedModels,
        int producerIndex,
        Optional<ResolvedProviderBoundary> boundary
    ) {
        if (orderedModels == null || producerIndex < 0 || producerIndex + 1 >= orderedModels.size()
            || boundary.isEmpty() || boundary.get().claim().stepContract().isEmpty()) {
            return Optional.empty();
        }
        PipelineStepModel producer = orderedModels.get(producerIndex);
        PipelineStepModel await = orderedModels.get(producerIndex + 1);
        if (producer.streamingShape() != StreamingShape.UNARY_STREAMING
            || await.streamingShape() != StreamingShape.UNARY_UNARY
            || !PipelineTargetResolutionPhase.AWAIT_STEP_DESCRIPTOR_CLASS
                .equals(await.serviceClassName().canonicalName())
            || !producer.outputMapping().domainType().equals(await.inputMapping().domainType())
            || !boundary.get().claim().stepContract().orElseThrow().capabilities()
                .contains(ProviderCapability.RESUMABLE_SOURCE)) {
            return Optional.empty();
        }
        return Optional.of(new Candidate(producerIndex, producer, await, terminalScalarSuffix(orderedModels, producerIndex + 1)));
    }

    private static boolean terminalScalarSuffix(List<PipelineStepModel> models, int awaitIndex) {
        // Empty suffix has no compiled terminal scalar continuation to run. It remains fail-closed.
        if (awaitIndex + 1 >= models.size()) {
            return false;
        }
        for (int index = awaitIndex + 1; index < models.size(); index++) {
            PipelineStepModel step = models.get(index);
            String serviceType = step.serviceClassName().canonicalName();
            if (step.streamingShape() != StreamingShape.UNARY_UNARY
                || PipelineTargetResolutionPhase.AWAIT_STEP_DESCRIPTOR_CLASS.equals(serviceType)
                || PipelineTargetResolutionPhase.COMMAND_STEP_DESCRIPTOR_CLASS.equals(serviceType)
                || PipelineTargetResolutionPhase.QUERY_STEP_DESCRIPTOR_CLASS.equals(serviceType)) {
                return false;
            }
        }
        return true;
    }

    public record Candidate(
        int producerStepIndex,
        PipelineStepModel producer,
        PipelineStepModel await,
        boolean terminalScalarSuffix
    ) {
    }
}
