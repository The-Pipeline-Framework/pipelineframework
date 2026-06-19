package org.pipelineframework.objectingest;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Queue async admission function used by the neutral object ingest runner.
 */
@FunctionalInterface
public interface ObjectExecutionAdmission {

    Uni<RunAsyncAcceptedDto> submit(Object input, String tenantId, String idempotencyKey);
}
