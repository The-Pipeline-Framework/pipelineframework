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
 * Claim-check reference to a payload stored outside the in-flight pipeline message.
 *
 * @param provider repository provider name, for example filesystem or s3
 * @param container provider-specific bucket/container/root identifier
 * @param key provider-specific object key
 * @param contentType stored payload content type
 * @param codec codec identifier used to encode the payload
 * @param checksum checksum value, typically sha-256 hex
 * @param sizeBytes encoded payload size in bytes
 * @param version version or replay tag associated with the payload
 * @param metadata provider/application metadata
 */
public record PayloadReference(
    String provider,
    String container,
    String key,
    String contentType,
    String codec,
    String checksum,
    long sizeBytes,
    String version,
    Map<String, String> metadata
) {
    public PayloadReference {
        provider = normalize(provider);
        container = normalize(container);
        key = normalize(key);
        contentType = normalize(contentType);
        codec = normalize(codec);
        checksum = normalize(checksum);
        version = normalize(version);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        if (key == null) {
            throw new IllegalArgumentException("payload reference key must not be blank");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("payload reference sizeBytes must be >= 0");
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
