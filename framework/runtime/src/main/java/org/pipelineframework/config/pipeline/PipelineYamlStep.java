/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.config.pipeline;

/**
 * Pipeline step entry parsed from pipeline.yaml.
 *
 * @param name the step name
 * @param kind the step kind, for example internal, delegated, remote, or await
 * @param cardinality the declared cardinality
 * @param inputType the input type name
 * @param inboundMapper the optional inbound mapper class name
 * @param outputType the output type name
 * @param outboundMapper the optional outbound mapper class name
 * @param timeout the await timeout, if this is an await step
 * @param idempotencyKeyFields fields used to derive await idempotency keys
 * @param awaitConfig await-step configuration, if this is an await step
 * @param queryId referenced query definition id, if this is a query step
 * @param queryCapture query capture settings, if this is a query step
 */
public record PipelineYamlStep(
    String name,
    String kind,
    String cardinality,
    String inputType,
    String inboundMapper,
    String outputType,
    String outboundMapper,
    String timeout,
    java.util.List<String> idempotencyKeyFields,
    PipelineYamlAwaitConfig awaitConfig,
    String queryId,
    PipelineYamlQueryCapture queryCapture
) {
    public PipelineYamlStep {
        kind = kind == null || kind.isBlank() ? "internal" : kind;
        cardinality = cardinality == null || cardinality.isBlank() ? "ONE_TO_ONE" : cardinality;
        idempotencyKeyFields = idempotencyKeyFields == null ? java.util.List.of() : java.util.List.copyOf(idempotencyKeyFields);
        queryCapture = queryCapture == null ? new PipelineYamlQueryCapture(java.util.List.of()) : queryCapture;
    }

    public PipelineYamlStep(
        String name,
        String inputType,
        String inboundMapper,
        String outputType,
        String outboundMapper
    ) {
        this(name, "internal", "ONE_TO_ONE", inputType, inboundMapper, outputType, outboundMapper, null, java.util.List.of(), null, null, null);
    }

    public PipelineYamlStep(String name, String inputType, String outputType) {
        this(name, "internal", "ONE_TO_ONE", inputType, null, outputType, null, null, java.util.List.of(), null, null, null);
    }
}
