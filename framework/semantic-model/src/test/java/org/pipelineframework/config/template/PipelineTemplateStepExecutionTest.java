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

package org.pipelineframework.config.template;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineTemplateStepExecutionTest {

    @Test
    void exposesRecordComponentsAndNormalizesExecutionMetadata() {
        PipelineTemplateRemoteTarget target = PipelineTemplateRemoteTarget.ofUrl(" https://example.test/run ");
        PipelineTemplateStepExecution execution = new PipelineTemplateStepExecution(
            " remote ", " operator ", " grpc ", 2500, target
        );

        assertEquals("REMOTE", execution.mode());
        assertEquals("operator", execution.operatorId());
        assertEquals("grpc", execution.protocol());
        assertEquals(2500, execution.timeoutMs());
        assertEquals(target, execution.target());
        assertTrue(execution.isRemote());
    }

    @Test
    void defaultsMissingModeAndAcceptsNullOptionalComponents() {
        PipelineTemplateStepExecution execution = new PipelineTemplateStepExecution(null, null, null, null, null);

        assertEquals("LOCAL", execution.mode());
        assertNull(execution.operatorId());
        assertNull(execution.protocol());
        assertNull(execution.timeoutMs());
        assertNull(execution.target());
        assertFalse(execution.isRemote());
    }

    @Test
    void blankMetadataNormalizesAndRemoteCheckIgnoresCase() {
        PipelineTemplateStepExecution execution = new PipelineTemplateStepExecution(" rEmOtE ", "  ", "", null, null);

        assertEquals("REMOTE", execution.mode());
        assertNull(execution.operatorId());
        assertNull(execution.protocol());
        assertTrue(execution.isRemote());
    }
}
