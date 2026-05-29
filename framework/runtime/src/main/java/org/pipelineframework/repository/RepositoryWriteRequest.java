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

import java.util.Map;

/**
 * Encoded payload write request for a repository provider.
 */
public record RepositoryWriteRequest(
    String container,
    String key,
    byte[] payload,
    String contentType,
    String codec,
    String checksum,
    String version,
    Map<String, String> metadata
) {
    public RepositoryWriteRequest {
        key = normalize(key);
        contentType = normalize(contentType);
        codec = normalize(codec);
        checksum = normalize(checksum);
        version = normalize(version);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        payload = payload == null ? new byte[0] : payload.clone();
        if (key == null) {
            throw new IllegalArgumentException("repository write key must not be blank");
        }
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
