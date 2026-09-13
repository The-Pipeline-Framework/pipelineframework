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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineTemplateRemoteTargetTest {

    @Test
    void recordComponentsAndFactoriesNormalizeTheirValues() {
        PipelineTemplateRemoteTarget literal = PipelineTemplateRemoteTarget.ofUrl(" https://example.test/run ");
        PipelineTemplateRemoteTarget configured = PipelineTemplateRemoteTarget.ofUrlConfigKey(" remote.url ");

        assertEquals("https://example.test/run", literal.url());
        assertNull(literal.urlConfigKey());
        assertNull(configured.url());
        assertEquals("remote.url", configured.urlConfigKey());
        assertEquals(new PipelineTemplateRemoteTarget("https://example.test/run", null), literal);
    }

    @Test
    void acceptsExactlyOneNonblankTargetComponent() {
        assertEquals("https://example.test/run",
            new PipelineTemplateRemoteTarget(" https://example.test/run ", null).url());
        assertEquals("remote.url",
            new PipelineTemplateRemoteTarget(null, " remote.url ").urlConfigKey());
    }

    @Test
    void rejectsMissingTargetAfterNullAndBlankNormalization() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateRemoteTarget(null, "  ")
        );

        assertEquals("PipelineTemplateRemoteTarget requires either url or urlConfigKey", exception.getMessage());
        assertThrows(IllegalArgumentException.class, () -> new PipelineTemplateRemoteTarget(null, null));
        assertThrows(IllegalArgumentException.class, () -> PipelineTemplateRemoteTarget.ofUrl("  "));
        assertThrows(IllegalArgumentException.class, () -> PipelineTemplateRemoteTarget.ofUrlConfigKey(null));
    }

    @Test
    void rejectsBothTargetComponentsWithCurrentMessage() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateRemoteTarget("https://example.test/run", "remote.url")
        );

        assertEquals("Specify either url or urlConfigKey, not both", exception.getMessage());
    }
}
