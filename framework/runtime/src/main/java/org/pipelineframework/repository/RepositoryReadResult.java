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

package org.pipelineframework.repository;

/**
 * Encoded payload read result from a repository provider.
 */
public record RepositoryReadResult(
    PayloadReference reference,
    byte[] payload,
    String contentType,
    String codec,
    String checksum
) {
    public RepositoryReadResult {
        if (reference == null) {
            throw new IllegalArgumentException("repository read reference must not be null");
        }
        payload = payload == null ? new byte[0] : payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
