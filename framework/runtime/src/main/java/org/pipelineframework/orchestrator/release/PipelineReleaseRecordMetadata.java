package org.pipelineframework.orchestrator.release;

import java.util.Objects;

final class PipelineReleaseRecordMetadata {

    private PipelineReleaseRecordMetadata() {
    }

    static boolean sameImmutableMetadata(PipelineReleaseRecord left, PipelineReleaseRecord right) {
        return Objects.equals(left.contractVersion(), right.contractVersion())
            && Objects.equals(left.releaseVersion(), right.releaseVersion())
            && Objects.equals(left.primaryArtifactId(), right.primaryArtifactId())
            && Objects.equals(left.primaryArtifactDigest(), right.primaryArtifactDigest())
            && Objects.equals(left.primaryArtifactUri(), right.primaryArtifactUri())
            && left.primaryArtifactSizeBytes() == right.primaryArtifactSizeBytes()
            && Objects.equals(left.primaryArtifactChecksum(), right.primaryArtifactChecksum())
            && Objects.equals(left.descriptor(), right.descriptor())
            && Objects.equals(left.contract(), right.contract());
    }
}
