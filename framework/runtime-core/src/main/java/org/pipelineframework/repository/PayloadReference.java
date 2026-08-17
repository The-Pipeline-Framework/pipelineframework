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
import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.connector.ConnectorPayloadOrigin;

/**
 * Portable claim-check reference to payload bytes owned by a repository or connector binding.
 *
 * <p>An empty connector origin denotes repository ownership and {@code provider} selects the
 * repository provider. A present connector origin delegates interpretation of provider-native
 * location fields to that exact binding-owned object-source operation.</p>
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
    Map<String, String> metadata,
    Optional<ConnectorPayloadOrigin> connectorOrigin
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
        connectorOrigin = connectorOrigin == null ? Optional.empty() : connectorOrigin;
        if (key == null) {
            throw new IllegalArgumentException("payload reference key must not be blank");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("payload reference sizeBytes must be >= 0");
        }
        if (connectorOrigin.isEmpty() && provider == null) {
            throw new IllegalArgumentException("repository-owned payload reference provider must not be blank");
        }
    }

    /**
     * Associates this payload reference with a connector payload origin.
     *
     * @param origin the connector origin associated with the payload
     * @return a new payload reference containing the specified connector origin
     */
    public PayloadReference withConnectorOrigin(ConnectorPayloadOrigin origin) {
        return new PayloadReference(
            provider, container, key, contentType, codec, checksum, sizeBytes, version, metadata,
            Optional.of(Objects.requireNonNull(origin, "connector payload origin must not be null")));
    }

    /**
     * Normalizes an optional string value by trimming it and converting blank values to {@code null}.
     *
     * @param value the string value to normalize
     * @return the trimmed value, or {@code null} if the input is absent or blank
     */
    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
