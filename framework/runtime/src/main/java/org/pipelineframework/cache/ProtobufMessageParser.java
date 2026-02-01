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
     * Fully qualified class name of the protobuf message.
     *
     * @return message type name
     */
    String type();

    /**
     * Parse the protobuf payload into a message instance.
     *
     * @param bytes serialized protobuf bytes
     * @return parsed message
     */
    Message parseFrom(byte[] bytes);
}
