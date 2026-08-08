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

package org.pipelineframework.csv.service;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Converts generated protobuf transport values to the JSON object carried by external await envelopes. */
final class PaymentProviderAwaitTransportPayload {
  private PaymentProviderAwaitTransportPayload() {
  }

  static Object protobufJson(Message payload) {
    try {
      return PipelineJson.mapper().readValue(JsonFormat.printer().print(payload), Object.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to encode generated protobuf await payload", e);
    }
  }
}
