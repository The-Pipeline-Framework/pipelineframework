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

package org.pipelineframework.csv.common.domain;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;

final class PaymentStatusJsonDeserializer extends JsonDeserializer<PaymentStatus> {

  @Override
  public PaymentStatus deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {
    JsonNode node = parser.getCodec().readTree(parser);
    if (node.hasNonNull("approved")) {
      return toApproved(node.get("approved"), parser);
    }
    if (node.hasNonNull("unapproved")) {
      return toUnapproved(node.get("unapproved"), parser);
    }
    String type = node.path("type").asText(null);
    if ("approved".equalsIgnoreCase(type)) {
      return toApproved(node, parser);
    }
    if ("unapproved".equalsIgnoreCase(type) || "rejected".equalsIgnoreCase(type)) {
      return toUnapproved(node, parser);
    }
    String status = node.path("status").asText(null);
    if ("Rejected".equalsIgnoreCase(status)) {
      return toUnapproved(node, parser);
    }
    if ("Complete".equalsIgnoreCase(status) || "Completed".equalsIgnoreCase(status)) {
      return toApproved(node, parser);
    }
    if (status != null && !status.isBlank()) {
      throw new IOException("Unsupported PaymentStatus status value: " + status);
    }
    throw new IOException(
        "Unsupported PaymentStatus payload: unable to resolve approved/unapproved branch");
  }

  private static ApprovedPaymentStatus toApproved(JsonNode node, JsonParser parser) throws IOException {
    ApprovedPaymentStatus status = new ApprovedPaymentStatus();
    populate(status, node, parser);
    return status;
  }

  private static UnapprovedPaymentStatus toUnapproved(JsonNode node, JsonParser parser) throws IOException {
    UnapprovedPaymentStatus status = new UnapprovedPaymentStatus();
    populate(status, node, parser);
    return status;
  }

  private static void populate(PaymentStatus status, JsonNode node, JsonParser parser) throws IOException {
    status.setId(uuid(node, "id"));
    status.setCustomerReference(text(node, "customerReference"));
    status.setReference(text(node, "reference"));
    status.setStatus(text(node, "status"));
    status.setMessage(text(node, "message"));
    status.setFee(decimal(node, "fee"));
    status.setConversationId(requiredUuid(node, "conversationId"));
    status.setStatusCode(longValue(node, "statusCode"));
    status.setPaymentRecord(paymentRecord(node, parser));
    status.setPaymentRecordId(requiredUuid(node, "paymentRecordId"));
  }

  private static PaymentRecord paymentRecord(JsonNode node, JsonParser parser) throws IOException {
    JsonNode paymentRecord = node.get("paymentRecord");
    if (paymentRecord == null || paymentRecord.isNull()) {
      return null;
    }
    return parser.getCodec().treeToValue(paymentRecord, PaymentRecord.class);
  }

  private static UUID requiredUuid(JsonNode node, String fieldName) throws IOException {
    UUID value = uuid(node, fieldName);
    if (value == null) {
      throw new IOException("Required PaymentStatus field '" + fieldName + "' is missing");
    }
    return value;
  }

  private static UUID uuid(JsonNode node, String fieldName) throws IOException {
    String value = text(node, fieldName);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid UUID for PaymentStatus field '" + fieldName + "': " + value, e);
    }
  }

  private static Long longValue(JsonNode node, String fieldName) throws IOException {
    JsonNode value = node.get(fieldName);
    if (value == null || value.isNull()) {
      return null;
    }
    if (value.isIntegralNumber()) {
      return value.longValue();
    }
    String text = value.asText(null);
    if (text == null || text.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      throw new IOException(
          "Invalid long for PaymentStatus field '" + fieldName + "': " + text, e);
    }
  }

  private static BigDecimal decimal(JsonNode node, String fieldName) throws IOException {
    JsonNode value = node.get(fieldName);
    if (value == null || value.isNull()) {
      return null;
    }
    if (value.isNumber()) {
      return value.decimalValue();
    }
    String text = value.asText(null);
    if (text == null || text.isBlank()) {
      return null;
    }
    try {
      return new BigDecimal(text);
    } catch (NumberFormatException e) {
      throw new IOException(
          "Invalid decimal for PaymentStatus field '" + fieldName + "': " + text, e);
    }
  }

  private static String text(JsonNode node, String fieldName) {
    JsonNode value = node.get(fieldName);
    return value == null || value.isNull() ? null : value.asText();
  }
}
