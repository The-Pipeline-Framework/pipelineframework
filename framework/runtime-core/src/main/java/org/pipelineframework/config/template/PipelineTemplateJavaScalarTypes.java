/*
 * Copyright (c) 2026 Mariano Barcia
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

/** Canonical Java target names for v3 scalar types. */
public final class PipelineTemplateJavaScalarTypes {

    public String typeName(String scalar) {
        return switch (scalar) {
            case "string" -> "String";
            case "bool" -> "Boolean";
            case "int32" -> "Integer";
            case "int64" -> "Long";
            case "float32" -> "Float";
            case "float64" -> "Double";
            case "decimal" -> "java.math.BigDecimal";
            case "uuid" -> "java.util.UUID";
            case "timestamp" -> "java.time.Instant";
            case "datetime" -> "java.time.LocalDateTime";
            case "date" -> "java.time.LocalDate";
            case "duration" -> "java.time.Duration";
            case "bytes" -> "com.google.protobuf.ByteString";
            case "currency" -> "java.util.Currency";
            case "uri" -> "java.net.URI";
            case "path" -> "java.nio.file.Path";
            case "payload_ref" -> "org.pipelineframework.repository.PayloadReference";
            default -> throw new IllegalStateException("Unsupported version 3 Java scalar '" + scalar + "'.");
        };
    }
}
