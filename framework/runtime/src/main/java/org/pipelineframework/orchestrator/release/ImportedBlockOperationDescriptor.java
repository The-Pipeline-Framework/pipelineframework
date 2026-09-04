package org.pipelineframework.orchestrator.release;

/** One provider operation referenced through an imported Block requirement. */
public record ImportedBlockOperationDescriptor(String id, int version) {
}
