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

package org.pipelineframework.connector;

import java.util.Locale;

/** Structural cardinality of a Query operation. */
public enum QueryOperationCardinality {
    ONE_TO_ONE,
    ONE_TO_MANY;

    static QueryOperationCardinality of(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("query operation cardinality must not be blank");
        }
        try {
            return valueOf(value.strip().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("unsupported Query operation cardinality: " + value, failure);
        }
    }
}
