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

package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.composition.PipelineCompositionContinuation;
import org.pipelineframework.orchestrator.composition.PipelineCompositionContinuationKind;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDefinition;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;
import org.pipelineframework.orchestrator.composition.PipelineCompositionNode;

class PipelineContractDescriptorLoaderTest {

    @Test
    void loadsTheCompositionFromThePinnedContractResource() throws Exception {
        PipelineCompositionDescriptor composition = new PipelineCompositionDescriptor("outer", List.of(
            new PipelineCompositionDefinition("outer", "definition-hash", "Value", "Value", List.of(
                new PipelineCompositionNode(0, "terminal", PipelineCompositionNode.DIRECT,
                    "Value", "Value", "ONE_TO_ONE", "")), List.of(
                new PipelineCompositionContinuation("terminal", PipelineCompositionContinuationKind.ROOT_TERMINAL, "")))));
        PipelineContractDescriptor descriptor = new PipelineContractDescriptor(
            3, "outer", "sha256:contract", "contract", null, null, null, false, null,
            List.of(), PipelineBundleCapabilities.defaults(), Map.of(), "", composition);

        byte[] json = PipelineJson.mapper().writeValueAsBytes(descriptor);
        PipelineContractDescriptor loaded = new PipelineContractDescriptorLoader().load(
            new ByteArrayInputStream(json));

        assertEquals(composition, loaded.composition());
        assertEquals("outer", loaded.composition().rootDefinitionId());
    }
}
