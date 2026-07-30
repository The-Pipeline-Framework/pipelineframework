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
