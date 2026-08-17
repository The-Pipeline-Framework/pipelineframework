/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.processor.routing;

import java.util.Objects;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.config.template.PipelineTemplateV3FlowCompatibility;
import org.pipelineframework.processor.composition.PipelineInvocationCompatibility;

/** Applies ordinary v3 flow and output assignability rules at a composition callsite. */
public final class V3PipelineInvocationCompatibility implements PipelineInvocationCompatibility {

    private final PipelineTemplateTypeModel typeModel;

    public V3PipelineInvocationCompatibility(PipelineTemplateTypeModel typeModel) {
        this.typeModel = Objects.requireNonNull(typeModel, "typeModel must not be null");
    }

    @Override
    public boolean inputCompatible(String callsiteInputContractId, String definitionInputContractId) {
        return PipelineTemplateV3FlowCompatibility.compatible(typeModel, callsiteInputContractId, definitionInputContractId);
    }

    @Override
    public boolean outputCompatible(String definitionOutputContractId, String callsiteOutputContractId) {
        return typeModel.isAssignable(definitionOutputContractId, callsiteOutputContractId);
    }
}
