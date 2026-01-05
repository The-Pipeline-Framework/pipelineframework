package org.pipelineframework.processor;

import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;

/**
 * Holds a pipeline step model with its resolved transport bindings.
 */
public record ResolvedStep(
        PipelineStepModel model,
        GrpcBinding grpcBinding,
        RestBinding restBinding
) {}
