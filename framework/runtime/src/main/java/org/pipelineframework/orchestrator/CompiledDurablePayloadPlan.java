package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Immutable release-bound encoder/decoder plan; construction is not a steady-state payload operation. */
public record CompiledDurablePayloadPlan(
    CanonicalPayloadBinding binding,
    ObjectWriter writer,
    ObjectReader reader
) {
    public static CompiledDurablePayloadPlan compile(CanonicalPayloadBinding binding) {
        return new CompiledDurablePayloadPlan(
            binding,
            PipelineJson.mapper().writerFor(binding.runtimeClass()),
            PipelineJson.mapper().readerFor(binding.runtimeClass()));
    }
}
