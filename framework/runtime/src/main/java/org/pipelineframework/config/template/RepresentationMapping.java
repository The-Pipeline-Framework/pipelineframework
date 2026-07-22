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

package org.pipelineframework.config.template;

import java.util.Optional;

/**
 * An optional external representation for one named v3 domain type.
 *
 * <p>The declaration is intentionally component-neutral. A consumer decides whether both optional
 * class names are required and how the mapping is resolved. It is not wire identity and must not
 * affect protobuf generation or IDL compatibility.</p>
 *
 * @param key component identifier that consumes the representation
 * @param domainType named v3 domain type
 * @param representationType optional external representation class name
 * @param mapperType optional {@code Mapper<Domain, Representation>} class name
 */
public record RepresentationMapping(
    String key,
    String domainType,
    Optional<String> representationType,
    Optional<String> mapperType
) {
    public RepresentationMapping {
        requireText(key, "key");
        requireText(domainType, "domainType");
        representationType = normalize(representationType);
        mapperType = normalize(mapperType);
    }

    private static Optional<String> normalize(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String normalized = value.get().trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("representation mapping class name must not be blank");
        }
        return Optional.of(normalized);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
