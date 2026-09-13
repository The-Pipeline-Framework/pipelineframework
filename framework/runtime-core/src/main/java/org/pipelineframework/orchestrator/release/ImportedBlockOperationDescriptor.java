package org.pipelineframework.orchestrator.release;

/** Shared identity for one provider operation referenced through an imported Block requirement. */
public record ImportedBlockOperationDescriptor(String id, int version) {
}
