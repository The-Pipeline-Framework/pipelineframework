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

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.repository.PayloadCodec;

@ApplicationScoped
public class BytesPayloadCodec implements PayloadCodec<byte[]> {
    @Override
    public String codecName() {
        return "bytes";
    }

    @Override
    public String contentType() {
        return "application/octet-stream";
    }

    @Override
    public boolean supports(Class<?> type) {
        return byte[].class.equals(type);
    }

    @Override
    public byte[] encode(byte[] value) {
        return value == null ? new byte[0] : value.clone();
    }

    @Override
    public byte[] decode(byte[] payload, Class<byte[]> type) {
        return payload == null ? new byte[0] : payload.clone();
    }
}
