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

package org.pipelineframework.envelope;

import java.util.Objects;

/**
 * Strict-control, loose-payload envelope for external step-host compatibility.
 */
public record TpfEnvelope(
    String protocolVersion,
    TpfEnvelopeControl control,
    TpfEnvelopePayload payload
) {
    public static final String PROTOCOL_VERSION = "tpf.envelope.v1";

    public TpfEnvelope {
        protocolVersion = protocolVersion == null || protocolVersion.isBlank() ? PROTOCOL_VERSION : protocolVersion.trim();
        if (!PROTOCOL_VERSION.equals(protocolVersion)) {
            throw new IllegalArgumentException("Unsupported TPF envelope protocolVersion '" + protocolVersion + "'");
        }
        control = Objects.requireNonNull(control, "control must not be null");
        payload = Objects.requireNonNull(payload, "payload must not be null");
    }
}
