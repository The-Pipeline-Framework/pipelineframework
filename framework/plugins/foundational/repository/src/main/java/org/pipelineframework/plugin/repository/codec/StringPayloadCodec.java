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

package org.pipelineframework.plugin.repository.codec;

import java.nio.charset.StandardCharsets;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.repository.PayloadCodec;

@ApplicationScoped
public class StringPayloadCodec implements PayloadCodec<String> {
    @Override
    public String codecName() {
        return "string";
    }

    @Override
    public String contentType() {
        return "text/plain; charset=utf-8";
    }

    @Override
    public boolean supports(Class<?> type) {
        return String.class.equals(type);
    }

    @Override
    public byte[] encode(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decode(byte[] payload, Class<String> type) {
        return new String(payload == null ? new byte[0] : payload, StandardCharsets.UTF_8);
    }
}
