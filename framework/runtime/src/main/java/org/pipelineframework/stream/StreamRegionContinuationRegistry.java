package org.pipelineframework.stream;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

/** Resolves a generated continuation by the provider-owned, release-pinned source descriptor. */
@ApplicationScoped
public class StreamRegionContinuationRegistry {

    @Inject
    Instance<StreamRegionContinuation> continuations;

    public Optional<StreamRegionContinuation> find(ResumableSourceDescriptor descriptor) {
        if (descriptor == null) {
            return Optional.empty();
        }
        StreamRegionContinuation match = null;
        for (StreamRegionContinuation candidate : continuations) {
            if (!descriptor.equals(candidate.descriptor())) {
                continue;
            }
            if (match != null) {
                throw new IllegalStateException("Multiple generated stream continuations match " + descriptor);
            }
            match = candidate;
        }
        return Optional.ofNullable(match);
    }

    /** Finds the one compiler-generated producer continuation for an ordered producer cursor. */
    public Optional<StreamRegionContinuation> findForProducerStep(int producerStepIndex) {
        if (producerStepIndex < 0) {
            return Optional.empty();
        }
        StreamRegionContinuation match = null;
        for (StreamRegionContinuation candidate : continuations) {
            if (candidate.producerStepIndex() != producerStepIndex) {
                continue;
            }
            if (match != null) {
                throw new IllegalStateException(
                    "Multiple generated stream continuations target producer step " + producerStepIndex);
            }
            match = candidate;
        }
        return Optional.ofNullable(match);
    }
}
