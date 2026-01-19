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

package org.pipelineframework.config.template;

import java.util.List;

/**
 * Step configuration from the pipeline template definition.
 *
 * @param name the step display name
 * @param cardinality the step cardinality (for example ONE_TO_ONE, EXPANSION)
 * @param inputTypeName the declared input type name
 * @param inputFields the input field definitions
 * @param outputTypeName the declared output type name
 * @param outputFields the output field definitions
 */
public record PipelineTemplateStep(
    String name,
    String cardinality,
    String inputTypeName,
    List<PipelineTemplateField> inputFields,
    String outputTypeName,
    List<PipelineTemplateField> outputFields
) {
}
