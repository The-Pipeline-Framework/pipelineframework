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

import java.util.Optional;

/**
 * Deferred-completion modifier parsed from an ordinary operation in pipeline.yaml.
 *
 * @param correlation correlation configuration
 * @param transport transport adapter configuration
 * @param completion optional request-aware completion projection
 */
public record PipelineYamlAwaitConfig(
    PipelineYamlAwaitCorrelation correlation,
    Optional<PipelineYamlAwaitTransport> transport,
    Optional<PipelineYamlAwaitCompletion> completion,
    Optional<PipelineYamlAwaitCallback> callback
) {
    public PipelineYamlAwaitConfig(PipelineYamlAwaitCorrelation correlation, PipelineYamlAwaitTransport transport,
        Optional<PipelineYamlAwaitCompletion> completion) {
        this(correlation, Optional.of(transport), completion, Optional.empty());
    }
    public PipelineYamlAwaitConfig(
        PipelineYamlAwaitCorrelation correlation,
        PipelineYamlAwaitTransport transport
    ) {
        this(correlation, transport, Optional.empty());
    }

    public PipelineYamlAwaitConfig {
        correlation = correlation == null ? new PipelineYamlAwaitCorrelation("interactionId") : correlation;
        java.util.Objects.requireNonNull(transport, "transport");
        java.util.Objects.requireNonNull(callback, "callback");
        if (transport.isPresent() == callback.isPresent()) {
            throw new IllegalArgumentException("await requires exactly one of transport or callback");
        }
        completion = completion == null ? Optional.empty() : completion;
    }
}
