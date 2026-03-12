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

package org.pipelineframework.cache;

import com.google.protobuf.Message;

/**
 * Supplies a non-reflective protobuf parser for a specific message type.
 */
public interface ProtobufMessageParser {

    /**
     * The protobuf schema full name for the message type, such as {@code package.Message}.
     *
     * @return the protobuf schema full name for this parser
     */
    String type();

    /**
     * Parses the provided protobuf payload into a Message instance.
     *
     * @param bytes the serialized protobuf bytes; must not be {@code null}
     * @return the parsed protobuf Message instance without mutating {@code bytes}
     * @throws NullPointerException if {@code bytes} is {@code null} and the implementation does not accept it
     * @throws RuntimeException if the payload is malformed or cannot be parsed into this message type
     */
    Message parseFrom(byte[] bytes);
}
