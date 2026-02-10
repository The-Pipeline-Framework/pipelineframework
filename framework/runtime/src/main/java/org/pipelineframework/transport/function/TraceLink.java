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

package org.pipelineframework.transport.function;

import java.util.Objects;

/**
 * Link to the previous lineage item.
 *
 * @param previousItemId previous lineage item identifier
 * @param previousPayloadInline optional inline payload representation
 * @param mode lineage persistence mode
 */
public record TraceLink(
    String previousItemId,
    Object previousPayloadInline,
    TraceLineageMode mode
) {
    /**
     * Creates a validated link.
     */
    public TraceLink {
        if (previousItemId == null || previousItemId.isBlank()) {
            throw new IllegalArgumentException("previousItemId is required");
        }
        if (mode == null) {
            mode = TraceLineageMode.REFERENCE;
        }
    }

    /**
     * Creates a reference-only lineage link.
     *
     * @param previousItemId previous item id
     * @return reference link
     */
    public static TraceLink reference(String previousItemId) {
        return new TraceLink(previousItemId, null, TraceLineageMode.REFERENCE);
    }

    /**
     * Creates an inline lineage link.
     *
     * @param previousItemId previous item id
     * @param previousPayloadInline inline previous payload
     * @return inline link
     */
    public static TraceLink inline(String previousItemId, Object previousPayloadInline) {
        Objects.requireNonNull(previousPayloadInline, "previousPayloadInline must not be null");
        return new TraceLink(previousItemId, previousPayloadInline, TraceLineageMode.INLINE);
    }
}
