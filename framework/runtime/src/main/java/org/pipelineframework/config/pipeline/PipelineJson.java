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

package org.pipelineframework.config.pipeline;

import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

/**
 * Shared ObjectMapper provider for pipeline JSON operations.
 */
public final class PipelineJson {

    private static final ObjectMapper MAPPER = createMapper();

    /**
     * Prevents instantiation of this utility class.
     */
    private PipelineJson() {
    }

    /**
     * Provide a copy of the configured ObjectMapper so callers cannot mutate the shared base instance.
     *
     * @return a new ObjectMapper copied from the class's configured shared mapper
     */
    public static ObjectMapper mapper() {
        return MAPPER.copy();
    }

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        SimpleModule protobufModule = new SimpleModule("pipeline-protobuf-json");
        protobufModule.addSerializer(MessageOrBuilder.class, new ProtobufJsonSerializer());
        mapper.registerModule(protobufModule);
        SimpleModule pathModule = new SimpleModule("pipeline-path-json");
        pathModule.addSerializer(Path.class, new PathJsonSerializer());
        pathModule.addDeserializer(Path.class, new PathJsonDeserializer());
        mapper.registerModule(pathModule);
        return mapper;
    }

    private static final class ProtobufJsonSerializer extends StdSerializer<MessageOrBuilder> {

        private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

        private ProtobufJsonSerializer() {
            super(MessageOrBuilder.class);
        }

        @Override
        public void serialize(MessageOrBuilder value, JsonGenerator generator, SerializerProvider provider)
            throws IOException {
            try {
                String json = PRINTER.print(value);
                if (generator.getCodec() instanceof ObjectMapper mapper) {
                    generator.writeTree(mapper.readTree(json));
                } else {
                    generator.writeRawValue(json);
                }
            } catch (Exception e) {
                throw new IOException("Failed to serialize protobuf message as JSON", e);
            }
        }
    }

    private static final class PathJsonSerializer extends StdSerializer<Path> {

        private PathJsonSerializer() {
            super(Path.class);
        }

        @Override
        public void serialize(Path value, JsonGenerator generator, SerializerProvider provider)
            throws IOException {
            generator.writeString(value.toString());
        }
    }

    private static final class PathJsonDeserializer extends StdDeserializer<Path> {

        private PathJsonDeserializer() {
            super(Path.class);
        }

        @Override
        public Path deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            String value = parser.getValueAsString();
            return value == null || value.isBlank() ? null : Path.of(value);
        }
    }
}
