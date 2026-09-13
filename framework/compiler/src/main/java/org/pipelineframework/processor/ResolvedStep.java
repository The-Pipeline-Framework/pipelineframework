package org.pipelineframework.processor;

import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;

/**
 * Holds a pipeline step model with its resolved transport bindings.
 *
 * @param model the resolved step model
 * @param grpcBinding optional gRPC binding for the step
 * @param restBinding optional REST binding for the step
 */
public record ResolvedStep(
        PipelineStepModel model,
        GrpcBinding grpcBinding,
        RestBinding restBinding
) {}
